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

        private int headerRow; //This is the 1-based row, counting from the first row of the CSV file, that contains the headers.

        private enum MetadataRow { UOM, InterpolationType, MaximumInterpolation, Description, Headers }

        private Dictionary<long, MetadataRow> metadataRows;
        private Dictionary<MetadataRow, string> metadataDefaults;

        private AssetOutputV1 rootAsset;

        private int? uomRow;
        private int? interpTypeRow;
        private string interpTypeDefault;
        private string maxInterpDefault;
        private int? maxInterpRow;
        private int? descriptionRow;
        private int firstDataRow;
        private string timestampFormat;
        private string timeZone;
        private bool enforceTimestampOrder;
        private bool useFilePathForHierarchy;
        private string filePathHierarchyRoot;
        private bool filePathHierarchyIncludesFilename;
        private string pathSeparator = @"\";
        private bool postInvalidSamplesInsteadOfSkipping;
        private int recordsPerDataPacket;
        private bool debugMode;
        private string scopedTo;
        private string filenameRegexScopeCapture;
        private string cultureInfoString;
        private string delimiter;
        private int maxFileSizeInKb;

        public string Name { get; set; }

        public TagsWithMetadataReaderV1(Dictionary<string, string> readerConfiguration, bool debugMode) {
            try {
                this.debugMode = debugMode;

                this.metadataRows = new Dictionary<long, MetadataRow>() {
                    { Convert.ToInt32(readerConfiguration["NameRow"]), MetadataRow.Headers }
                };
                if (readerConfiguration.ContainsKey("UOMRow")) {
                    this.metadataRows.Add(Convert.ToInt32(readerConfiguration["UOMRow"]), MetadataRow.UOM);
                }
                if (readerConfiguration.ContainsKey("InterpStyleRow")) {
                    this.metadataRows.Add(Convert.ToInt32(readerConfiguration["InterpStyleRow"]), MetadataRow.InterpolationType);
                }
                if (readerConfiguration.ContainsKey("MaxInterpRow")) {
                    this.metadataRows.Add(Convert.ToInt32(readerConfiguration["MaxInterpRow"]), MetadataRow.MaximumInterpolation);
                }
                if (readerConfiguration.ContainsKey("DescriptionRow")) {
                    this.metadataRows.Add(Convert.ToInt32(readerConfiguration["DescriptionRow"]), MetadataRow.Description);
                }

                this.metadataDefaults = new Dictionary<MetadataRow, string>();
                this.metadataDefaults.Add(MetadataRow.MaximumInterpolation,
                    readerConfiguration.ContainsKey("DefaultMaxInterp") ? readerConfiguration["DefaultMaxInterp"] :
                    null);
                this.metadataDefaults.Add(MetadataRow.InterpolationType,
                    readerConfiguration.ContainsKey("DefaultInterpType") ? readerConfiguration["DefaultInterpType"] :
                    null);
                this.metadataDefaults.Add(MetadataRow.UOM,
                    readerConfiguration.ContainsKey("DefaultUOM") ? readerConfiguration["DefaultUOM"] :
                    null);
                this.metadataDefaults.Add(MetadataRow.Description,
                    readerConfiguration.ContainsKey("DefaultDescription") ? readerConfiguration["DefaultDescription"] :
                    null);

                this.headerRow = Convert.ToInt32(readerConfiguration["NameRow"]);
                this.firstDataRow = Convert.ToInt32(readerConfiguration["FirstDataRow"]); // Moved to top for clarity, since it's required
                this.uomRow = readerConfiguration.ContainsKey("UOMRow") ? // Cnanged the int to int?, which is shorthand for Nullable<int>,
                    Convert.ToInt32(readerConfiguration["UOMRow"]) :      // meaning it .HasValue (or not); the value can be accessed using .Value
                    (int?)null;
                this.interpTypeRow = readerConfiguration.ContainsKey("InterpStyleRow") ? // same
                    Convert.ToInt32(readerConfiguration["InterpStyleRow"]) :             // multilined for readability
                    (int?)null;
                this.maxInterpRow = readerConfiguration.ContainsKey("MaxInterpRow") ? // same
                    Convert.ToInt32(readerConfiguration["MaxInterpRow"]) :
                    (int?)null;
                this.descriptionRow = readerConfiguration.ContainsKey("DescriptionRow") ? // same
                    Convert.ToInt32(readerConfiguration["DescriptionRow"]) :
                    (int?)null;
                this.maxInterpDefault = readerConfiguration["DefaultMaxInterp"];
                this.interpTypeDefault = readerConfiguration["DefaultInterpType"];
                this.recordsPerDataPacket = Convert.ToInt32(readerConfiguration["RecordsPerDataPacket"]);
                if (this.headerRow >= this.firstDataRow) {
                    throw new ArgumentException("FirstDataRow must be greater than NameRow.");
                }
                this.timestampFormat = readerConfiguration["TimestampFormat"];
                this.timeZone = readerConfiguration["TimeZone"];
                if (bool.TryParse(readerConfiguration["EnforceTimestampOrder"], out this.enforceTimestampOrder) == false) {
                    this.enforceTimestampOrder = true;
                };
                this.useFilePathForHierarchy = Convert.ToBoolean(readerConfiguration["UseFilePathForHierarchy"]);
                this.filePathHierarchyRoot = readerConfiguration["FilePathHierarchyRoot"];
                this.filePathHierarchyIncludesFilename = Convert.ToBoolean(readerConfiguration["FilePathHierarchyIncludesFilename"]);
                this.postInvalidSamplesInsteadOfSkipping = readerConfiguration.ContainsKey("PostInvalidSamplesInsteadOfSkipping") ?
                    Convert.ToBoolean(readerConfiguration["PostInvalidSamplesInsteadOfSkipping"]) : false;
                this.scopedTo = readerConfiguration.ContainsKey("ScopedTo") ? readerConfiguration["ScopedTo"] : null;
                this.filenameRegexScopeCapture = readerConfiguration.ContainsKey("FilenameRegexScopeCapture") ?
                    readerConfiguration["FilenameRegexScopeCapture"] : null;
                if (this.scopedTo != null && this.filenameRegexScopeCapture != null) {
                    throw new ArgumentException("ScopedTo and FilenameRegexScopeCapture cannot both be non-null.");
                }
                this.cultureInfoString = readerConfiguration.ContainsKey("CultureInfo") ?
                    readerConfiguration["CultureInfo"] : null;
                this.delimiter = readerConfiguration.ContainsKey("Delimiter") ?
                    readerConfiguration["Delimiter"] : ",";

                this.maxFileSizeInKb = readerConfiguration.TryGetValue(nameof(BaseReaderConfig.MaxFileSizeInKB), out var rawMaxFileSizeInKb)
                    ? Convert.ToInt32(rawMaxFileSizeInKb)
                    : 5120; // 5MB
            } catch (ArgumentException ax) {
                log.Error(ax.Message, ax);
                throw;
            } catch (Exception ex) {
                string readerConfig = string.Join(",\n", readerConfiguration.Select(x => x.Key + ": " + x.Value));
                log.Error($"Failed to create TimestampTagsCsvReader due to exception: {ex.Message}\n" +
                    "The reader is expecting the following values for the configuration: TimestampHeader, TimestampFormat, HeaderRow, " +
                    "FirstDataRow, TimeZone, EnforceTimestampOrder, UseFilePathForHierarchy, FilePathHierarchyRoot, and FilePathHierarchyIncludesFilename.\n" +
                    "The reader configuration found in the config file was as follows:\n{readerConfig}", ex);
                throw;
            }
        }

        // This method should only be used for setting up things that are common to all files;
        // therefore there is no dependence on the filenames of the files being read.
        public override bool Initialize() {
            if (debugMode) {
                System.Diagnostics.Debugger.Launch();
            }
            try {
                if (this.useFilePathForHierarchy) {
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
                    rootAsset = DirectoryWatchUtilities.SetRootAsset(this.Connection, this.filePathHierarchyRoot, this.scopedTo);
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
            if (debugMode) {
                System.Diagnostics.Debugger.Launch();
            }
            log.Info($"Method ReadFile called for file {filename}");

            this.validateFileSizeLimit(log, this.maxFileSizeInKb, filename);

            // Prechecks:  ensure the signal configurations all exist as columns in the file,
            // confirm the data exists where specified for this reader (e.g., rows starting at N),
            // and check the timestamp and number formats.

            string fileScopedTo;

            if (this.filenameRegexScopeCapture != null) {
                Regex regex = new Regex(this.filenameRegexScopeCapture);
                try {
                    MatchCollection matches = regex.Matches(filename);
                    if (matches.Count == 1) {
                        if (matches[0].Groups.Count == 2) {
                            fileScopedTo = matches[0].Groups[1].Value;
                        } else {
                            throw new ArgumentException($"Regex {filenameRegexScopeCapture} found more than one capture for filename {filename}");
                        }
                    } else {
                        throw new ArgumentException($"Regex {filenameRegexScopeCapture} found more than one match for filename {filename}");
                    }
                } catch {
                    log.Error($"Failed to use regex {filenameRegexScopeCapture} of Reader configuration to capture the Workbook Scope for file {filename}.");
                    throw;
                }
            } else {
                fileScopedTo = this.scopedTo;
            }

            string assetPath = "";
            if (this.useFilePathForHierarchy) {
                try {
                    assetPath = filename.Substring(filename.LastIndexOf(this.filePathHierarchyRoot));
                    if (this.filePathHierarchyIncludesFilename) {
                        assetPath = assetPath.Substring(0, assetPath.LastIndexOf('.'));
                    } else {
                        assetPath = assetPath.Substring(0, assetPath.LastIndexOf('\\'));
                    }
                } catch (Exception ex) {
                    log.Error($"Failed to create asset path for file {filename} due to exception: {ex.Message}", ex);
                    throw ex;
                }
            }

            TextFieldParser parser;
            try {
                parser = new TextFieldParser(filename);
                parser.TextFieldType = FieldType.Delimited;
                parser.SetDelimiters(this.delimiter);
                parser.TrimWhiteSpace = false;
            } catch (Exception ex) {
                string errMsg = string.Format("Failed to open CSV parser in DateTimeTagsCsvReader for file: {0} due to exception {1}", filename, ex.Message);
                throw new Exception(errMsg);
            }
            long firstMetadataRow = this.metadataRows.Keys.Min();
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

            //create dictionary for each, or add to dictionary? imagine dictionary where each item has all signalConfiguration? I think I like this
            // yes!
            Dictionary<MetadataRow, List<string>> metadata = new Dictionary<MetadataRow, List<string>>();
            foreach (MetadataRow row in ((MetadataRow[])Enum.GetValues(typeof(MetadataRow)))) {
                metadata.Add(row, null);
            }
            //List<string> headers = new List<string> { };
            //List<string> eachUOM = new List<string> { };
            //List<string> eachInterpType = new List<string> { };
            //List<string> eachMaxInterp = new List<string> { };
            //List<string> eachDescription = new List<string> { };
            //populate lists in order discovered
            for (long i = firstMetadataRow; i < firstDataRow; i++) {
                if (parser.EndOfData == false) {
                    if (this.metadataRows.ContainsKey(parser.LineNumber)) {
                        MetadataRow metadataRow = this.metadataRows[parser.LineNumber];
                        List<string> metadataForRow = parser.ReadFields().ToList();
                        metadata[metadataRow] =
                            metadataForRow.Select(x => string.IsNullOrEmpty(x)
                              ? this.metadataDefaults[metadataRow] : x
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
            foreach (MetadataRow key in metadata.Keys.ToArray()) {
                if (metadata[key] == null) {
                    metadata[key] = metadata[MetadataRow.Headers].Select(x => metadataDefaults[key]).ToList();
                }
            }

            //create index from headers once made into a list
            Dictionary<string, int> signalHeaderIndices = new Dictionary<string, int>();
            List<SignalConfigurationV1> signalConfigurations = new List<SignalConfigurationV1>();
            int count = 0;
            var dupcheck = new HashSet<string>();
            foreach (var signalHeader in metadata[MetadataRow.Headers]) {
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

                    if (metadata.ContainsKey(MetadataRow.Description)) {
                        signalConfiguration.Description = metadata[MetadataRow.Description][count];
                    }
                    if (metadata.ContainsKey(MetadataRow.UOM)) {
                        signalConfiguration.Uom = metadata[MetadataRow.UOM][count];
                    }
                    if (metadata.ContainsKey(MetadataRow.MaximumInterpolation)) {
                        signalConfiguration.MaximumInterpolation = metadata[MetadataRow.MaximumInterpolation][count];
                    }
                    if (metadata.ContainsKey(MetadataRow.InterpolationType)) {
                        signalConfiguration.InterpolationType = metadata[MetadataRow.InterpolationType][count];
                    }

                    signalConfigurations.Add(signalConfiguration);
                    count++;
                }
            }

            // Since we know the amount of data is going to be small, we can safely (and inefficiently) read the
            // CSV data columns into memory along the way.  Some form of batching will exist in the next version

            int recordCounter = 0;
            long lineNumber = firstDataRow;
            CultureInfo cultureInfo = this.cultureInfoString != null ?
                new CultureInfo(this.cultureInfoString) : null;
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
                if (DateTime.TryParseExact(rowFields[timestampHeaderIndex], this.timestampFormat, null, System.Globalization.DateTimeStyles.None, out formatTimeWithDate)) {
                    if (string.IsNullOrWhiteSpace(this.timeZone)) {
                        if (timezoneWarningIssued == false) {
                            log.Warn("No TimeZone specified - C# will assign time zone automatically.");
                            timezoneWarningIssued = true;
                        }
                        timestamp = rowFields[timestampHeaderIndex];
                    } else {
                        timestamp = string.Format("{0} {1}", rowFields[timestampHeaderIndex], this.timeZone);
                    }
                } else {
                    throw new Exception($"On line {lineNumber} of file {filename}, " +
                        $"timestamp {rowFields[timestampHeaderIndex]} did not conform to format " +
                        $"{timestampFormat}");
                }

                if (DateTime.TryParse(timestamp, cultureInfo, DateTimeStyles.None, out timestampAsDateTime)) {
                    timestampIsoString = timestampAsDateTime.ToString("o");
                    if (timestampAsDateTime.Kind == DateTimeKind.Unspecified) {
                        timestampIsoString += this.timeZone;
                    }
                } else {
                    string errMsg = $"Failed to parse date time timezone string {timestamp} concatenated from column {timestampHeaderIndex} " +
                        $"and configured TimeZone {this.timeZone} for file {filename}";
                    parser.Close();
                    parser.Dispose();
                    throw new Exception(errMsg);
                }
                if (this.enforceTimestampOrder &&
                    DateTime.Parse(timestampIsoString).ToUniversalTime() < DateTime.Parse(previousTimestamp).ToUniversalTime()) {
                    string errMsg = string.Format("Found out of order timestamps {0}, {1} in file {2}", previousTimestamp, timestampIsoString, filename);
                    parser.Close();
                    parser.Dispose();
                    throw new Exception(errMsg);
                }
                foreach (string signalHeader in signalHeaderIndices.Keys) {
                    string seeqSignalPath = "";
                    if (this.useFilePathForHierarchy) {
                        seeqSignalPath = assetPath + pathSeparator;
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
                        } else if (postInvalidSamplesInsteadOfSkipping) {
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

                if (recordCounter == this.recordsPerDataPacket) {
                    DirectoryWatchData data = new DirectoryWatchData {
                        Connection = this.Connection,
                        Filename = filename,
                        PathSeparator = pathSeparator,
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
                    PathSeparator = pathSeparator,
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