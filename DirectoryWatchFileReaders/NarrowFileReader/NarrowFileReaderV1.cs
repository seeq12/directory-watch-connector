using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using Microsoft.VisualBasic.FileIO;
using Seeq.Link.Connector.DirectoryWatch.Config;
using Seeq.Link.Connector.DirectoryWatch.Interfaces;
using Seeq.Link.Connector.DirectoryWatch.Utilities;
using Seeq.Link.SDK.Interfaces;
using Seeq.Sdk.Api;
using Seeq.Sdk.Model;

namespace Seeq.Link.Connector.DirectoryWatch.DataFileReaders {

    public class NarrowFileReaderV1 : DataFileReader {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private readonly object lockObj = new object();

        private string pathSeparator = @"\";

        private CultureInfo cultureInfo;

        public NarrowFileReaderConfigV1 ReaderConfiguration { get; set; }

        public string Name { get; set; }

        public NarrowFileReaderV1(Dictionary<string, string> readerConfiguration, bool debugMode = false) {
            try {
                this.ReaderConfiguration = new NarrowFileReaderConfigV1(readerConfiguration, debugMode);
            } catch (Exception ex) {
                log.Error($"Failed to configure TimestampTagsCsvReaderV1 due to exception: {ex.Message}", ex);
            }
        }

        // This method should only be used for setting up things that are common to all files;
        // therefore there is no dependence on the filenames of the files being read.
        public override bool Initialize() {
            if (this.ReaderConfiguration.DebugMode) {
                System.Diagnostics.Debugger.Launch();
            }
            this.cultureInfo = this.ReaderConfiguration.CultureInfo != null ?
                new CultureInfo(this.ReaderConfiguration.CultureInfo) : null;
            return true;
        }

        public override void ReadFile(string filename) {
            log.InfoFormat("Method ReadFile called for file {0}", filename);

            // Prechecks:  ensure the signal configurations all exist as columns in the file,
            // confirm the data exists where specified for this reader (e.g., rows starting at N),
            // and check the timestamp and number formats.

            TextFieldParser parser;
            try {
                parser = new TextFieldParser(filename);
                parser.TextFieldType = FieldType.Delimited;
                parser.SetDelimiters(this.ReaderConfiguration.Delimiter);
                parser.TrimWhiteSpace = false;
            } catch (Exception ex) {
                string errMsg = string.Format("Failed to open CSV parser in DateTimeTagsCsvReader for file: {0} due to exception {1}", filename, ex.Message);
                throw new Exception(errMsg);
            }

            for (int i = 1; i < this.ReaderConfiguration.HeaderRow; i++) {
                if (parser.EndOfData == false) {
                    parser.ReadLine(); // We ignore everything before the header row
                } else {
                    string errMsg = string.Format("Ran out of rows before reaching header row for file named {0}", filename);
                    parser.Close();
                    parser.Dispose();
                    throw new Exception(errMsg);
                }
            }
            List<string> headers;
            if (parser.EndOfData == false) {
                headers = parser.ReadFields().ToList();
            } else {
                string errMsg = string.Format("Ran out of rows before reaching header row for file named {0}", filename);
                parser.Close();
                parser.Dispose();
                throw new Exception(errMsg);
            }

            List<int> timestampHeaderIndices = new List<int>();
            foreach (string timestampHeader in this.ReaderConfiguration.TimestampHeaders.Split(',')) {
                try {
                    timestampHeaderIndices.Add(DirectoryWatchUtilities.GetHeaderIndex(timestampHeader.Trim(), headers));
                } catch (Exception ex) {
                    parser.Close();
                    parser.Dispose();
                    throw ex;
                }
            }

            int signalNameHeaderIndex = DirectoryWatchUtilities.GetHeaderIndex(this.ReaderConfiguration.SignalNameHeader, headers);
            int valueHeaderIndex = DirectoryWatchUtilities.GetHeaderIndex(this.ReaderConfiguration.ValueHeader, headers);

            // Read down to the start of data

            for (int i = this.ReaderConfiguration.HeaderRow; i < this.ReaderConfiguration.FirstDataRow - 1; i++) {
                if (parser.EndOfData == false) {
                    parser.ReadLine(); // We ignore everything between the header row and the first data row
                } else {
                    string errMsg = string.Format("Ran out of rows before reaching first data row for file named {0}", filename);
                    parser.Close();
                    parser.Dispose();
                    throw new Exception(errMsg);
                }
            }

            Dictionary<string, List<SampleInputV1>> seeqSignalData = new Dictionary<string, List<SampleInputV1>>();
            List<string> timestamps = new List<string>();
            string timestamp;
            string previousTimestamp = "1970-01-02T00:00:00Z";
            int recordCounter = 0;
            while (parser.EndOfData == false) {
                List<string> rowFields = parser.ReadFields().ToList();
                // Some CSV files contain a "soft EOF" demarkated by the substitute character 0xA1 (integer value = 26)
                // If there's only one think on the line and it's 0xA1, break out of the loop.
                if (rowFields.Count == 1) {
                    if (Encoding.ASCII.GetBytes(rowFields[0])[0] == BitConverter.GetBytes(26)[0]) {
                        break;
                    }
                }
                DateTime timestampAsDateTime;
                string timestampIsoString;
                foreach (int timestampHeaderIndex in timestampHeaderIndices) {
                    if (string.IsNullOrWhiteSpace(rowFields[timestampHeaderIndex])) {
                        // stop reading file if we run out of timestamps, but don't error out; move on to writing data to Seeq
                        log.Debug($"Missing timestamp column value in column {headers[timestampHeaderIndex]} " +
                            $"after reading {recordCounter} records in file {filename}");
                        break;
                    }
                }
                string timestampColumnsJoined = string.Join(" ", timestampHeaderIndices.Select(i => rowFields[i]));

                // Try parsing the timestamp without the timezone to confirm it is actually conformant to the user's configured timestampFormat.
                // We ppend the timeZoneFormatAffix (if applicable), and if the parse is successful, we convert the DateTime to an ISO8601 string
                timestamp = string.IsNullOrWhiteSpace(this.ReaderConfiguration.TimeZone) ?
                    timestampColumnsJoined :
                    string.Format("{0} {1}", timestampColumnsJoined, this.ReaderConfiguration.TimeZone);
                string timeZoneFormatAffix = string.IsNullOrWhiteSpace(this.ReaderConfiguration.TimeZone) ? "" : " zzz";
                if (DateTime.TryParseExact(timestamp, this.ReaderConfiguration.TimestampFormat +
                    timeZoneFormatAffix, cultureInfo, DateTimeStyles.None, out timestampAsDateTime)) {
                    if (timestampAsDateTime.Kind == DateTimeKind.Unspecified) {
                        log.Warn($"Timestamp {timestamp} was parsed as a DateTime with Unspecified DateTimeKind.");
                    }
                    timestampIsoString = timestampAsDateTime.ToUniversalTime().ToString("o");
                } else {
                    parser.Close();
                    parser.Dispose();
                    throw new Exception($"Timestamp {timestampColumnsJoined} joined from columns specified by" +
                        $" timestampHeaders did not conform to format {this.ReaderConfiguration.TimestampFormat} " +
                        $"in file {filename}");
                }

                string signalUom = null;
                string seeqSignalPath = this.ReaderConfiguration.SignalPrefixForDisambiguation + rowFields[signalNameHeaderIndex];
                if (seeqSignalData.ContainsKey(seeqSignalPath) == false) {
                    seeqSignalData[seeqSignalPath] = new List<SampleInputV1>();
                }
                string sampleValue;

                sampleValue = rowFields[valueHeaderIndex];

                // In case the CultureInfo specified for the file is not the same as the agent's host,
                // try to convert the string sampleValue read by the parser into a format known to the machine
                long numLong;
                double numDouble;
                bool validLong =
                    long.TryParse(sampleValue.ToString(), NumberStyles.Any, this.cultureInfo, out numLong)
                    && signalUom != "string";
                bool validDouble = double.TryParse(sampleValue.ToString(), NumberStyles.Any, this.cultureInfo, out numDouble)
                    && signalUom != "string";
                bool validSampleValue =
                    signalUom == "string" ^ (validLong || validDouble);
                if (validSampleValue) {
                    if (validLong) {
                        sampleValue = numLong.ToString(); // this is the step that converts to the CurrentCulture
                    } else if (validDouble) {
                        sampleValue = numDouble.ToString(); // this is the step that converts to the CurrentCulture
                    }
                    SampleInputV1 sample = DirectoryWatchUtilities.SeeqSample(timestampIsoString, sampleValue);
                    if (sample != null) {
                        seeqSignalData[seeqSignalPath].Add(sample);
                    }
                } else if (this.ReaderConfiguration.PostInvalidSamplesInsteadOfSkipping) {
                    string nullString = null;
                    seeqSignalData[seeqSignalPath].Add(new SampleInputV1 { Key = timestampIsoString, Value = nullString });
                }

                recordCounter++;
                if (recordCounter % this.ReaderConfiguration.RecordsPerDataPacket == 0) {
                    log.Info($"Sending {this.ReaderConfiguration.RecordsPerDataPacket} rows of data to Seeq; recordCount is {recordCounter}");
                    List<SignalConfigurationV1> signalConfigurations = seeqSignalData.Keys.Select(x =>
                        new SignalConfigurationV1 {
                            NameInFile = x,
                            NameInSeeq = x,
                            Description = null,
                            InterpolationType = "linear",
                            MaximumInterpolation = null,
                            Required = true,
                            Uom = null
                        }).ToList();
                    DirectoryWatchData data = new DirectoryWatchData() {
                        SeeqSignalData = seeqSignalData,
                        Connection = this.Connection,
                        Filename = filename,
                        PathSeparator = this.pathSeparator,
                        SignalConfigurations = signalConfigurations
                    };
                    DirectoryWatchUtilities.SendData(data, true, true, true);
                    seeqSignalData.Clear();
                }
                previousTimestamp = timestampIsoString;
            }

            if (seeqSignalData.Count > 0) {
                List<SignalConfigurationV1> signalConfigurations = seeqSignalData.Keys.Select(x =>
                    new SignalConfigurationV1 {
                        NameInFile = x,
                        NameInSeeq = x,
                        Description = null,
                        InterpolationType = "linear",
                        MaximumInterpolation = null,
                        Required = true,
                        Uom = null
                    }).ToList();
                DirectoryWatchData data = new DirectoryWatchData() {
                    SeeqSignalData = seeqSignalData,
                    Connection = this.Connection,
                    Filename = filename,
                    PathSeparator = this.pathSeparator,
                    SignalConfigurations = signalConfigurations
                };
                DirectoryWatchUtilities.SendData(data, true, true, true);
            }

            parser.Close();
            parser.Dispose();

            log.InfoFormat("Completed reading all data from file {0}; sending data to Seeq database", filename);

            this.Connection.MetadataSync(SyncMode.Full);
        }
    }
}