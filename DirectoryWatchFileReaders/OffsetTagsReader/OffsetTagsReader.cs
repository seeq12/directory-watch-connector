using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.VisualBasic.FileIO;
using Seeq.Link.Connector.DirectoryWatch;
using Seeq.Link.Connector.DirectoryWatch.Config;
using Seeq.Link.Connector.DirectoryWatch.Interfaces;
using Seeq.Link.Connector.DirectoryWatch.Utilities;
using Seeq.Link.SDK.Interfaces;
using Seeq.Sdk.Model;

namespace OffsetTagsReader {

    public class OffsetTagsReader : DataFileReader {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private bool debugMode;

        private string pathSeparator = Path.DirectorySeparatorChar.ToString();

        private AssetOutputV1 rootAsset;
        private OffsetTagsReaderConfig readerConfig;

        public string Name { get; set; }

        public OffsetTagsReader(Dictionary<string, string> readerConfiguration, bool debugMode = false) {
            try {
                this.readerConfig = new OffsetTagsReaderConfig(readerConfiguration);
                this.debugMode = debugMode;
            } catch (Exception ex) {
                log.Error($"Failed to configure OffsetTagsReaderV1 due to exception: {ex.Message}", ex);
                this.readerConfig = null;
            }
        }

        // This method should only be used for setting up things that are common to all files;
        // there should be no dependence on the filenames of the files being read.
        public override bool Initialize() {
            if (this.debugMode) {
                System.Diagnostics.Debugger.Launch();
            }
            try {
                rootAsset = DirectoryWatchUtilities.SetRootAsset(this.Connection, readerConfig.AssetTreeRootName, readerConfig.ScopedTo);
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

            // Prechecks:  ensure the signal configurations all exist as columns in the file,
            // confirm the data exists where specified for this reader (e.g., rows starting at N),
            // and check the timestamp and number formats.

            DateTime baseDateTime;

            if (string.IsNullOrWhiteSpace(this.readerConfig.FixedBaseTime)) {
                // Try to capture experiment start time from the filename with a regex - if this fails, the file cannot be read
                MatchCollection filenameDateMatches = (new Regex(this.readerConfig.FilenameDateCaptureRegex))
                    .Matches(Path.GetFileNameWithoutExtension(filename));
                DateTime filenameDateWithTimeZone;
                if (filenameDateMatches.Count == 1 && filenameDateMatches[0].Groups.Count == 2) {
                    // exactly one match and one capture
                    string filenameDateCapture = filenameDateMatches[0].Groups[1].Value;
                    DateTime filenameDateDateTime;
                    if (DateTime.TryParseExact(filenameDateCapture, this.readerConfig.FilenameDateFormatString,
                        System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out filenameDateDateTime)) {
                        if (string.IsNullOrWhiteSpace(this.readerConfig.TimeZone)) {
                            log.Warn("No TimeZone specified - C# will assign time zone automatically.");
                            filenameDateWithTimeZone = filenameDateDateTime;
                        } else {
                            filenameDateWithTimeZone = DateTime.ParseExact($"{filenameDateCapture}{this.readerConfig.TimeZone}",
                                this.readerConfig.FilenameDateFormatString + "K", null, System.Globalization.DateTimeStyles.None).ToUniversalTime();
                        }
                        baseDateTime = filenameDateWithTimeZone;
                    } else {
                        throw new Exception($"For file {filename}, " +
                            $"Captured date {filenameDateCapture} did not conform to format " +
                            $"{this.readerConfig.FilenameDateFormatString} with specified TimeZone {this.readerConfig.TimeZone ?? "[null]"}");
                    }
                } else {
                    throw new ArgumentException("Filename timestamp capture failed to find a unique match or a unique capture.");
                }
            } else {
                // Note that we've already confirmed in the ReaderConfig constructor that FixedBaseTime is an ISO string.
                baseDateTime = DateTime.Parse(this.readerConfig.FixedBaseTime);
            }

            string assetPath;
            if (this.readerConfig.UseFilePathForHierarchy) {
                if (filename.LastIndexOf(this.readerConfig.FilePathHierarchyRoot) != -1) {
                    // set the base assetPath to the file path hierarchy root and replace the root as necessary
                    assetPath = filename.Substring(filename.LastIndexOf(this.readerConfig.FilePathHierarchyRoot));
                    List<string> nodes = assetPath.Split(new string[] { pathSeparator }, StringSplitOptions.None).Skip(1).ToList();
                    List<string> updatedNodes = new List<string> { this.readerConfig.AssetTreeRootName };
                    updatedNodes.AddRange(nodes);
                    assetPath = string.Join(pathSeparator, updatedNodes);
                } else {
                    string msg = $"FilePathHierarchyRoot must be a directory in the path to any directory specified" +
                        $" in the FileDirectories list of the configuration, but the FilePathHierarchyRoot was set to " +
                        $"{readerConfig.FilePathHierarchyRoot}, which is not contained in the absolute path for this file:" +
                        $"{filename}";
                    throw new ArgumentException(msg);
                }
                if (this.readerConfig.UseFilenameRegexForLeafAsset) {
                    if (this.readerConfig.FilenameAssetCaptureRegex == null) {
                        // truncate at filename sans extension; whole filename used since no regex
                        assetPath = assetPath.Substring(0, Math.Max(assetPath.LastIndexOf('.'), assetPath.Length));
                    } else {
                        MatchCollection filenameAssetMatches =
                            (new Regex(this.readerConfig.FilenameAssetCaptureRegex))
                            .Matches(Path.GetFileNameWithoutExtension(filename));
                        if (filenameAssetMatches.Count == 1 && filenameAssetMatches[0].Groups.Count == 2) {
                            // match is unique and has a unique capture group (aside from the match itself)
                            string assetName = filenameAssetMatches[0].Groups[1].Value;
                            assetPath = assetPath.Substring(0, assetPath.LastIndexOf(pathSeparator) + 1) + assetName;
                        } else {
                            throw new ArgumentException("Filename asset capture failed to find a unique match or a unique capture.");
                        }
                    }
                } else {
                    assetPath = assetPath.LastIndexOf(pathSeparator) == -1 ? assetPath :
                        assetPath.Substring(0, assetPath.LastIndexOf(pathSeparator));
                }
            } else {
                assetPath = readerConfig.AssetTreeRootName; // just use the file path root if not using file path for hierarchy
            }

            if (rootAsset.DataId == null) {
                throw new NullReferenceException($"The value of assetTreeRootDataId is not set, so read of file {filename} " +
                    $"cannot proceed.");
            }

            Dictionary<string, List<SampleInputV1>> seeqSignalData;

            TextFieldParser parser;
            try {
                parser = new TextFieldParser(filename);
                parser.TextFieldType = FieldType.Delimited;
                parser.SetDelimiters(",");
                parser.TrimWhiteSpace = false;
            } catch (Exception ex) {
                string errMsg = $"Failed to open CSV parser in DateTimeTagsCsvReader for file: {filename} " +
                    $"due to exception {ex.Message}";
                throw new Exception(errMsg);
            }

            try {
                while (parser.LineNumber < readerConfig.HeaderRow) {
                    if (parser.EndOfData == false) {
                        parser.ReadLine(); // ignore these lines
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

                int timeSpanHeaderIndex = -1; // only to avoid a compile error
                bool timeSpanIsFixed = string.IsNullOrWhiteSpace(readerConfig.TimeSpanHeader);
                if (!timeSpanIsFixed) {
                    try {
                        timeSpanHeaderIndex = DirectoryWatchUtilities.GetHeaderIndex(readerConfig.TimeSpanHeader, headers);
                    } catch (Exception ex) {
                        parser.Close();
                        parser.Dispose();
                        throw ex;
                    }
                }

                Dictionary<string, int> signalHeaderIndices = new Dictionary<string, int>();
                foreach (SignalConfigurationV1 signalConfig in this.SignalConfigurations) {
                    string signalNameInFile = signalConfig.NameInFile;
                    try {
                        signalHeaderIndices[signalNameInFile] = DirectoryWatchUtilities.GetHeaderIndex(signalNameInFile, headers);
                    } catch (Exception ex) {
                        if (signalConfig.Required) {
                            parser.Close();
                            parser.Dispose();
                            throw ex;
                        }
                    }
                }

                // Read down to the start of data
                while (parser.LineNumber < readerConfig.FirstDataRow) {
                    if (parser.EndOfData == false) {
                        parser.ReadLine(); // We ignore everything between the header row and the first data row
                    } else {
                        string errMsg = string.Format("Ran out of rows before reaching first data row for file named {0}", filename);
                        parser.Close();
                        parser.Dispose();
                        throw new Exception(errMsg);
                    }
                }

                int recordCounter = 0;
                long lineNumber = -1;
                string previousTimestamp = "1970-01-02T00:00:00Z";
                List<string> timestamps = new List<string>();
                seeqSignalData = new Dictionary<string, List<SampleInputV1>>();

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
                    string timestampIsoString;
                    TimeSpan timeSpanParsed;
                    DateTime dateTimeForRow;
                    if (!timeSpanIsFixed && string.IsNullOrWhiteSpace(rowFields[timeSpanHeaderIndex])) {
                        // stop reading file if we run out of timestamps, but don't error out; move on to writing data to Seeq
                        log.Warn($"Found row with missing timestamp at line {lineNumber} of file {filename}; " +
                            $"stopping read operation, but previous data rows may already have been sent to Seeq - check preceding logs.");
                        break;
                    }
                    double testTimeSpanRowValue;
                    if (!timeSpanIsFixed && double.TryParse(rowFields[timeSpanHeaderIndex], out testTimeSpanRowValue) == false) {
                        throw new IOException($"On line {lineNumber} of file {filename}, timespan column value " +
                            $"{rowFields[timeSpanHeaderIndex]} could not be converted to an integer.");
                    }
                    double timeSpanValue = timeSpanIsFixed ? readerConfig.TimeSpanFixed * (lineNumber - readerConfig.FirstDataRow)
                        : Convert.ToDouble(rowFields[timeSpanHeaderIndex]);
                    switch (readerConfig.TimeSpanUnits) {
                        case "Days":
                            timeSpanParsed = TimeSpan.FromDays((double)timeSpanValue);
                            break;

                        case "Hours":
                            timeSpanParsed = TimeSpan.FromHours((double)timeSpanValue);
                            break;

                        case "Minutes":
                            timeSpanParsed = TimeSpan.FromMinutes((double)timeSpanValue);
                            break;

                        case "Seconds":
                            timeSpanParsed = TimeSpan.FromSeconds((double)timeSpanValue);
                            break;

                        case "Milliseconds":
                            timeSpanParsed = TimeSpan.FromMilliseconds((double)timeSpanValue);
                            break;

                        case "Microseconds":
                            timeSpanParsed = TimeSpan.FromTicks(10 * (Int64)timeSpanValue);
                            break;

                        case "Nanoseconds":
                            timeSpanParsed = TimeSpan.FromTicks((Int64)timeSpanValue / 100); // ROUNDING!
                            break;

                        default:
                            throw new ArgumentException($"Unrecognized TimeSpanUnits: {readerConfig.TimeSpanUnits}");
                    }
                    dateTimeForRow = baseDateTime + timeSpanParsed;

                    timestampIsoString = dateTimeForRow.ToString("o");

                    if (this.readerConfig.EnforceTimestampOrder &&
                        DateTime.Parse(timestampIsoString).ToUniversalTime() < DateTime.Parse(previousTimestamp).ToUniversalTime()) {
                        string errMsg = $"Found out of order timestamps {previousTimestamp}, {timestampIsoString} in file {filename}";
                        parser.Close();
                        parser.Dispose();
                        throw new Exception(errMsg);
                    }

                    foreach (string signalHeader in signalHeaderIndices.Keys) {
                        string seeqSignalPath = assetPath + pathSeparator;
                        seeqSignalPath +=
                            SignalConfigurations.Find(x => x.NameInFile == signalHeader).NameInSeeq;
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
                                (this.SignalConfigurations.Find(x => x.NameInFile == signalHeader).Uom == "string" ^
                                 (long.TryParse(sample.Value.ToString(), out numLong) ||
                                 double.TryParse(sample.Value.ToString(), out numDouble)));
                            if (validSample) {
                                seeqSignalData[seeqSignalPath].Add(sample);
                            } else if (this.readerConfig.PostInvalidSamplesInsteadOfSkipping) {
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
                    if (recordCounter == this.readerConfig.RecordsPerDataPacket) {
                        DirectoryWatchSignalData signalData = new DirectoryWatchSignalData {
                            ConnectionService = this.Connection,
                            Filename = filename,
                            PathSeparator = pathSeparator,
                            SignalConfigurations = this.SignalConfigurations,
                            SeeqSignalData = seeqSignalData,
                            ScopedTo = this.readerConfig.ScopedTo
                        };
                        if (DirectoryWatchUtilities.SendData(signalData, this.readerConfig.SkipBadSamples, this.readerConfig.SkipNullValueSamples)) {
                            log.Info($"Successfully posted data for the {recordCounter} records ending on line {lineNumber}");
                            seeqSignalData.Clear();
                            recordCounter = 0;
                        } else {
                            log.Error("Failed to send data packet to Seeq; read operation cancelled.");
                            throw new ArgumentException($"Failed call to SendData for file {filename}; last line read was {lineNumber}");
                        }
                    }
                }

                if (recordCounter > 0) {
                    log.Info($"Sending last batch of data to Seeq for file {filename}");
                    DirectoryWatchSignalData signalData = new DirectoryWatchSignalData {
                        ConnectionService = this.Connection,
                        Filename = filename,
                        PathSeparator = pathSeparator,
                        SignalConfigurations = this.SignalConfigurations,
                        SeeqSignalData = seeqSignalData,
                        ScopedTo = this.readerConfig.ScopedTo
                    };
                    if (DirectoryWatchUtilities.SendData(signalData, this.readerConfig.SkipBadSamples, this.readerConfig.SkipNullValueSamples)) {
                        log.Info($"Successfully posted data for the {recordCounter} records ending on line {lineNumber}");
                        seeqSignalData.Clear();
                        recordCounter = 0;
                    } else {
                        log.Error("Failed to send data packet to Seeq; read operation cancelled.");
                        throw new ArgumentException($"Failed call to SendData for file {filename}; last line read was {lineNumber}");
                    }
                }

                log.Info($"Completed reading all data from file {filename} ({lineNumber} rows).");
            } catch (Exception) {
                throw;
            } finally {
                parser.Close();
                parser.Dispose();
            }

            // Now that all prechecks have been passed, look up each signal configuration in Seeq.  If it exists, use it.
            // If not, create it.  Each signal has two custom properties that must be set:
            // DatastoreStatus and LastStoredTimestamp.  The former may be set to
            // Active, Sealed, or Reset.  If Active, the LastStoredTimestamp will
            // be used to determine how far into the file to start (the last stored timestamp
            // should generally only be needed from one signal in the file, but the oldest is
            // selected from all signal configurations to be certain).  It is assumed that
            // timestamps in the file are ALWAYS ascending!  A violation of this may definitely
            // break the reader.  If the DatastoreStatus is Sealed, no more changes are expected
            // to this file, so an error will be raised and no data will be imported for the signal.
            // This status is used on file delete to ensure that a new file added to the directory with
            // the exact same filename does not have its data added to an existing signal from a previous import.
            // Finally, if the status is Reset, the LastStoredTimestamp property will be reset, ensuring
            // all data is read for the signal.  One drawback of the current approach is that properties
            // stored on the signal are used to control the read, but it is likely that manual changes
            // to the DatastoreStatus would be intended to be applied across the whole file.  One
            // potential fix for this would be to add three more states ActiveAll, SealedAll, and ResetAll
            // that could be used to initiate a file-global status change.

            // If any new signals were created, initiate a metadata sync.  In doing so, query Seeq for
            // all signals that match this datasource and simply return a count.
            this.Connection.MetadataSync(SyncMode.Full);
        }
    }
}