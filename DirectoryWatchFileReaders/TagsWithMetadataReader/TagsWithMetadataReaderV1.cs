using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.VisualBasic.FileIO;
using Seeq.Link.Connector.DirectoryWatch.Config;
using Seeq.Link.Connector.DirectoryWatch.Interfaces;
using Seeq.Link.Connector.DirectoryWatch.Utilities;
using Seeq.Link.SDK.Interfaces;
using Seeq.Sdk.Api;
using Seeq.Sdk.Model;

namespace Seeq.Link.Connector.DirectoryWatch.DataFileReaders {

    public class TagsWithMetadataReaderV1 : DataFileReader {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private AssetOutputV1 rootAsset;
        private bool debugMode;

        public TagsWithMetadataReaderConfigV1 ReaderConfiguration { get; set; }

        public string Name { get; set; }

        public TagsWithMetadataReaderV1(Dictionary<string, string> readerConfiguration, bool debugMode) {
            try {
                this.ReaderConfiguration = new TagsWithMetadataReaderConfigV1(readerConfiguration, debugMode);
                this.debugMode = this.ReaderConfiguration.DebugMode;
            } catch (Exception ex) {
                log.Error($"Failed to configure TagsWithMetadataReaderConfigV1 due to exception: {ex.Message}", ex);
            }
        }

        // This method should only be used for setting up things that are common to all files;
        // therefore there is no dependence on the filenames of the files being read.
        public override bool Initialize() {
            if (this.debugMode) {
                System.Diagnostics.Debugger.Launch();
            }

            try {
                if (this.ReaderConfiguration.UseFilePathForHierarchy) {
                    IItemsApi itemsApi = this.Connection.AgentService.ApiProvider.CreateItemsApi();
                    ISignalsApi signalsApi = this.Connection.AgentService.ApiProvider.CreateSignalsApi();
                    UsersApi usersApi = new UsersApi(signalsApi.Configuration.ApiClient);
                    UserOutputV1 user = usersApi.GetUserFromUsername("Auth", "Seeq", "agent_api_key");
                    if (user.IsAdmin.HasValue && user.IsAdmin.Value == true) {
                        log.Info($"agent_api_key user is an admin; ScopedTo and FilenameRegexScopeCapture parameters " +
                            "may be used to scope the imported data to particular workbooks.");
                    } else {
                        log.Warn("agent_api_key user is NOT an admin; ScopedTo and FilenameRegexScopeCapture cannot be " +
                            "used until the agent_api_key user is granted admin status by an existing Seeq Administrator" +
                            "through the API Reference.");
                    }
                    this.rootAsset = DirectoryWatchUtilities.SetRootAsset(this.Connection, this.ReaderConfiguration.FilePathHierarchyRoot, this.ReaderConfiguration.ScopedTo);
                } else {
                    throw new ArgumentException("UseFilePathForHierarchy must be true for TagsWithMetadataReader");
                }
                return true;
            } catch (Exception ex) {
                log.Error($"Reader initialization failed: {ex.Message}", ex);
                return false;
            }
        }

        public override void ReadFile(string filename) {
            if (this.debugMode) {
                System.Diagnostics.Debugger.Launch();
            }

            log.Info($"Method ReadFile called for file {filename}");

            this.validateFileSizeLimit(log, this.ReaderConfiguration.MaxFileSizeInKB, filename);

            // Prechecks:  ensure the signal configurations all exist as columns in the file,
            // confirm the data exists where specified for this reader (e.g., rows starting at N),
            // and check the timestamp and number formats.

            string fileScopedTo;

            if (this.ReaderConfiguration.FilenameRegexScopeCapture != null) {
                Regex regex = new Regex(this.ReaderConfiguration.FilenameRegexScopeCapture);
                try {
                    MatchCollection matches = regex.Matches(filename);
                    if (matches.Count == 1) {
                        if (matches[0].Groups.Count == 2) {
                            fileScopedTo = matches[0].Groups[1].Value;
                        } else {
                            throw new ArgumentException($"Regex {this.ReaderConfiguration.FilenameRegexScopeCapture} found more than one capture for filename {filename}");
                        }
                    } else {
                        throw new ArgumentException($"Regex {this.ReaderConfiguration.FilenameRegexScopeCapture} found more than one match for filename {filename}");
                    }
                } catch {
                    log.Error($"Failed to use regex {this.ReaderConfiguration.FilenameRegexScopeCapture} of Reader configuration to capture the Workbook Scope for file {filename}.");
                    throw;
                }
            } else {
                fileScopedTo = this.ReaderConfiguration.ScopedTo;
            }

            string assetPath = "";
            if (this.ReaderConfiguration.UseFilePathForHierarchy) {
                try {
                    assetPath = filename.Substring(filename.LastIndexOf(this.ReaderConfiguration.FilePathHierarchyRoot));
                    if (this.ReaderConfiguration.FilePathHierarchyIncludesFilename) {
                        assetPath = assetPath.Substring(0, assetPath.LastIndexOf('.'));
                    } else {
                        assetPath = assetPath.Substring(0, assetPath.LastIndexOf('\\'));
                    }
                } catch (Exception ex) {
                    log.Error($"Failed to create asset path for file {filename} due to exception: {ex.Message}", ex);
                    throw;
                }
            }

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
            long firstMetadataRow = this.ReaderConfiguration.MetadataRows.Keys.Min();
            for (int i = 1; i < firstMetadataRow; i++) {
                if (parser.EndOfData == false) {
                    parser.ReadLine(); // We ignore everything before the first row of interest row
                } else {
                    string errMsg = string.Format("Ran out of rows before reaching header row for file named {0}", filename);
                    parser.Close();
                    parser.Dispose();
                    throw new Exception(errMsg);
                }
            }

            int timestampHeaderIndex = 0;

            Dictionary<TagsWithMetadataReaderConfigV1.MetadataRow, List<string>> metadata = new Dictionary<TagsWithMetadataReaderConfigV1.MetadataRow, List<string>>();
            foreach (TagsWithMetadataReaderConfigV1.MetadataRow row in ((TagsWithMetadataReaderConfigV1.MetadataRow[])Enum.GetValues(typeof(TagsWithMetadataReaderConfigV1.MetadataRow)))) {
                metadata.Add(row, null);
            }

            // populate lists in order discovered
            for (long i = firstMetadataRow; i < this.ReaderConfiguration.FirstDataRow; i++) {
                if (parser.EndOfData == false) {
                    if (this.ReaderConfiguration.MetadataRows.ContainsKey(parser.LineNumber)) {
                        TagsWithMetadataReaderConfigV1.MetadataRow metadataRow = this.ReaderConfiguration.MetadataRows[parser.LineNumber];
                        List<string> metadataForRow = parser.ReadFields().ToList();
                        metadata[metadataRow] =
                            metadataForRow.Select(x => string.IsNullOrEmpty(x)
                              ? this.ReaderConfiguration.MetadataDefaults[metadataRow] : x
                            ).ToList();
                    } else {
                        // throw it away!
                        var garbage = parser.ReadFields().ToList();
                    }
                } else {
                    string errMsg = string.Format("Ran out of rows before reaching first data row for file named {0}", filename);
                    parser.Close();
                    parser.Dispose();
                    throw new Exception(errMsg);
                }
            }

            // Create a list populated entirely by default values if no values already exist
            foreach (TagsWithMetadataReaderConfigV1.MetadataRow key in metadata.Keys.ToArray()) {
                if (metadata[key] == null) {
                    metadata[key] = metadata[TagsWithMetadataReaderConfigV1.MetadataRow.Headers].Select(x => this.ReaderConfiguration.MetadataDefaults[key]).ToList();
                }
            }

            //create index from headers once made into a list
            Dictionary<string, int> signalHeaderIndices = new Dictionary<string, int>();
            List<SignalConfigurationV1> signalConfigurations = new List<SignalConfigurationV1>();
            int count = 0;
            var dupcheck = new HashSet<string>();
            foreach (var signalHeader in metadata[TagsWithMetadataReaderConfigV1.MetadataRow.Headers]) {
                SignalConfigurationV1 signalConfiguration = new SignalConfigurationV1();
                //check for duplicate headers
                if (!dupcheck.Add(signalHeader)) {
                    string errMsg = string.Format("Headers contain duplicates: " + signalHeader);
                    throw new Exception(errMsg);
                    //break;
                }
                if (count == timestampHeaderIndex) {
                    count++;
                } else {
                    signalHeaderIndices.Add(signalHeader, count);
                    //Assign signalConfiguration
                    signalConfiguration.NameInSeeq = signalHeader;
                    signalConfiguration.NameInFile = signalHeader;

                    if (metadata.ContainsKey(TagsWithMetadataReaderConfigV1.MetadataRow.Description)) {
                        signalConfiguration.Description = metadata[TagsWithMetadataReaderConfigV1.MetadataRow.Description][count];
                    }
                    if (metadata.ContainsKey(TagsWithMetadataReaderConfigV1.MetadataRow.UOM)) {
                        signalConfiguration.Uom = metadata[TagsWithMetadataReaderConfigV1.MetadataRow.UOM][count];
                    }
                    if (metadata.ContainsKey(TagsWithMetadataReaderConfigV1.MetadataRow.MaximumInterpolation)) {
                        signalConfiguration.MaximumInterpolation = metadata[TagsWithMetadataReaderConfigV1.MetadataRow.MaximumInterpolation][count];
                    }
                    if (metadata.ContainsKey(TagsWithMetadataReaderConfigV1.MetadataRow.InterpolationType)) {
                        signalConfiguration.InterpolationType = metadata[TagsWithMetadataReaderConfigV1.MetadataRow.InterpolationType][count];
                    }

                    signalConfigurations.Add(signalConfiguration);
                    count++;
                }
            }

            // Since we know the amount of data is going to be small, we can safely (and inefficiently) read the
            // CSV data columns into memory along the way.  Some form of batching will exist in the next version

            int recordCounter = 0;
            long lineNumber = this.ReaderConfiguration.FirstDataRow;
            CultureInfo cultureInfo = this.ReaderConfiguration.CultureInfoString != null ?
                new CultureInfo(this.ReaderConfiguration.CultureInfoString) : null;
            Dictionary<string, List<SampleInputV1>> seeqSignalData = new Dictionary<string, List<SampleInputV1>>();
            List<string> timestamps = new List<string>();
            string timestamp;
            string previousTimestamp = "1970-01-02T00:00:00Z";
            bool timezoneWarningIssued = false;
            while (parser.EndOfData == false) {
                lineNumber = parser.LineNumber;
                List<string> rowFields = parser.ReadFields().ToList();
                // Some CSV files contain a "soft EOF" demarkated by the substitue character 0xA1 (integer value = 26)
                // If there's only one think on the line and it's 0xA1, break out of the loop.
                if (rowFields.Count == 1) {
                    if (Encoding.ASCII.GetBytes(rowFields[0])[0] == BitConverter.GetBytes(26)[0]) {
                        break;
                    }
                }
                DateTime timestampAsDateTime;
                string timestampIsoString;
                DateTime formatTimeWithDate;
                if (string.IsNullOrWhiteSpace(rowFields[timestampHeaderIndex])) {
                    // stop reading file if we run out of timestamps, but don't error out; move on to writing data to Seeq
                    log.Debug($"Reached last row with date and time values after reading {recordCounter} records in file {filename}");
                    break;
                }
                if (DateTime.TryParseExact(rowFields[timestampHeaderIndex], this.ReaderConfiguration.TimestampFormat, null, System.Globalization.DateTimeStyles.None, out formatTimeWithDate)) {
                    if (string.IsNullOrWhiteSpace(this.ReaderConfiguration.Zone)) {
                        if (timezoneWarningIssued == false) {
                            log.Warn("No TimeZone specified - C# will assign time zone automatically.");
                            timezoneWarningIssued = true;
                        }
                        timestamp = rowFields[timestampHeaderIndex];
                    } else {
                        timestamp = string.Format("{0} {1}", rowFields[timestampHeaderIndex], this.ReaderConfiguration.Zone);
                    }
                } else {
                    throw new Exception($"On line {lineNumber} of file {filename}, " +
                        $"timestamp {rowFields[timestampHeaderIndex]} did not conform to format " +
                        $"{this.ReaderConfiguration.TimestampFormat}");
                }

                if (DateTime.TryParse(timestamp, cultureInfo, DateTimeStyles.None, out timestampAsDateTime)) {
                    timestampIsoString = timestampAsDateTime.ToString("o");
                    if (timestampAsDateTime.Kind == DateTimeKind.Unspecified) {
                        timestampIsoString += this.ReaderConfiguration.Zone;
                    }
                } else {
                    string errMsg = $"Failed to parse date time timezone string {timestamp} concatenated from column {timestampHeaderIndex} " +
                        $"and configured TimeZone {this.ReaderConfiguration.Zone} for file {filename}";
                    parser.Close();
                    parser.Dispose();
                    throw new Exception(errMsg);
                }
                if (this.ReaderConfiguration.EnforceTimestampOrder &&
                    DateTime.Parse(timestampIsoString).ToUniversalTime() < DateTime.Parse(previousTimestamp).ToUniversalTime()) {
                    string errMsg = string.Format("Found out of order timestamps {0}, {1} in file {2}", previousTimestamp, timestampIsoString, filename);
                    parser.Close();
                    parser.Dispose();
                    throw new Exception(errMsg);
                }
                foreach (string signalHeader in signalHeaderIndices.Keys) {
                    string seeqSignalPath = "";
                    if (this.ReaderConfiguration.UseFilePathForHierarchy) {
                        seeqSignalPath = assetPath + this.ReaderConfiguration.PathSeparator;
                    }
                    seeqSignalPath +=
                        signalHeader;
                    if (seeqSignalData.ContainsKey(seeqSignalPath) == false) {
                        seeqSignalData[seeqSignalPath] = new List<SampleInputV1>();
                    }

                    string sampleValue;
                    if (rowFields.Count > signalHeaderIndices[signalHeader]) {
                        sampleValue = rowFields[signalHeaderIndices[signalHeader]];
                        SampleInputV1 sample = DirectoryWatchUtilities.SeeqSample(timestampIsoString, sampleValue);
                        long numLong;
                        double numDouble;
                        bool validSample = sample != null &&
                            (signalConfigurations.Find(x => x.NameInFile == signalHeader).Uom == "string" ^
                             (long.TryParse(sample.Value.ToString(), NumberStyles.Any, cultureInfo, out numLong) ||
                             double.TryParse(sample.Value.ToString(), NumberStyles.Any, cultureInfo, out numDouble)));
                        if (validSample) {
                            seeqSignalData[seeqSignalPath].Add(sample);
                        } else if (this.ReaderConfiguration.PostInvalidSamplesInsteadOfSkipping) {
                            string nullString = null;
                            seeqSignalData[seeqSignalPath].Add(new SampleInputV1 { Key = timestampIsoString, Value = nullString });
                        }
                    } else {
                        log.Warn($"On line {lineNumber} of file {filename}, " +
                            $"not enough columns were found to read value for field named {signalHeader}, "
                            + $"which was expected in column {signalHeaderIndices[signalHeader]}");
                        continue;
                    }
                }
                recordCounter++;
                previousTimestamp = timestampIsoString;

                if (recordCounter == this.ReaderConfiguration.RecordsPerDataPacket) {
                    DirectoryWatchData data = new DirectoryWatchData {
                        Connection = this.Connection,
                        Filename = filename,
                        PathSeparator = this.ReaderConfiguration.PathSeparator,
                        SignalConfigurations = signalConfigurations,
                        SeeqSignalData = seeqSignalData,
                        ScopedTo = fileScopedTo
                    };
                    if (DirectoryWatchUtilities.SendData(data)) {
                        log.Info($"Successfully posted data for the {recordCounter} records ending on line {parser.LineNumber}");
                        seeqSignalData.Clear();
                        recordCounter = 0;
                    } else {
                        log.Error("Failed to send data packet to Seeq; read operation cancelled.");
                        throw new ArgumentException($"Failed call to SendData for file {filename}; last line read was {parser.LineNumber}");
                    }
                }
            }

            if (recordCounter > 0) {
                log.Info($"Sending last batch of data to Seeq for file {filename}");
                DirectoryWatchData data = new DirectoryWatchData {
                    Connection = this.Connection,
                    Filename = filename,
                    PathSeparator = this.ReaderConfiguration.PathSeparator,
                    SignalConfigurations = signalConfigurations,
                    SeeqSignalData = seeqSignalData,
                    ScopedTo = fileScopedTo
                };
                if (DirectoryWatchUtilities.SendData(data)) {
                    log.Info($"Successfully posted data for the {recordCounter} records ending on line {parser.LineNumber}");
                    seeqSignalData.Clear();
                    recordCounter = 0;
                } else {
                    log.Error("Failed to send data packet to Seeq; read operation cancelled.");
                    throw new ArgumentException($"Failed call to SendData for file {filename}; last line read was {parser.LineNumber}");
                }
            }

            log.Info($"Completed reading all data from file {filename} ({lineNumber} rows).");

            parser.Close();
            parser.Dispose();

            // If any new signals were created, initiate a signalConfiguration sync.  In doing so, query Seeq for
            // all signals that match this datasource and simply return a count.
            this.Connection.MetadataSync(SyncMode.Full);
        }
    }
}