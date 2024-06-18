using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.VisualBasic.FileIO;
using Seeq.Sdk.Api;
using Seeq.Sdk.Model;
using Seeq.Link.SDK.Interfaces;
using Seeq.Link.Connector.DirectoryWatch.Config;
using Seeq.Link.Connector.DirectoryWatch.Utilities;
using Seeq.Link.Connector.DirectoryWatch.Interfaces;

namespace Seeq.Link.Connector.DirectoryWatch.DataFileReaders {

    public class ConditionsWithPropertiesReaderV1 : DataFileReader {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private readonly object lockObj = new object();

        private string pathSeparator = @"\";

        private string assetTreeRootDataId;
        private string assetTreeRootName;

        public ConditionsWithPropertiesReaderConfigV1 ReaderConfiguration { get; set; }

        public string Name { get; set; }

        public ConditionsWithPropertiesReaderV1(Dictionary<string, string> readerConfiguration, bool debugMode = false) {
            try {
                this.ReaderConfiguration = new ConditionsWithPropertiesReaderConfigV1(readerConfiguration, debugMode);
            } catch (Exception ex) {
                log.Error($"Failed to configure ConditionsWithPropertiesReaderV1 due to exception: {ex.Message}", ex);
            }
        }

        // This method should only be used for setting up things that are common to all files;
        // therefore there is no dependence on the filenames of the files being read.
        public override bool Initialize() {
            if (this.ReaderConfiguration.DebugMode) {
                System.Diagnostics.Debugger.Launch();
            }
            try {
                if (this.ReaderConfiguration.UseFilePathForHierarchy) {
                    IItemsApi itemsApi = this.Connection.AgentService.ApiProvider.CreateItemsApi();
                    // Since DataID is ignored by appserver for storedInSeeq signals, we use description as a proxy for this
                    // for now.  This means the current version ignores the user-specified Description in the ConditionConfigurations

                    IDatasourcesApi datasourcesApi = this.Connection.AgentService.ApiProvider.CreateDatasourcesApi();
                    DatasourceOutputV1 datasourceOutput = DirectoryWatchUtilities.GetDirectoryWatchDatasource(this.Connection);
                    string datasourceId = datasourceOutput.Id;
                    IAssetsApi assetsApi = this.Connection.AgentService.ApiProvider.CreateAssetsApi();
                    ITreesApi treesApi = this.Connection.AgentService.ApiProvider.CreateTreesApi();

                    string dataId = Utilities.DirectoryWatchUtilities.GuidifyString(this.ReaderConfiguration.FilePathHierarchyRoot);

                    AssetBatchInputV1 assetBatchInput = new AssetBatchInputV1 {
                        HostId = datasourceId,
                        Assets = new List<PutAssetInputV1> {
                            new PutAssetInputV1 {
                                Name = this.ReaderConfiguration.FilePathHierarchyRoot,
                                HostId = datasourceId,
                                DataId = dataId
                            }
                        }
                    };
                    lock (this.lockObj) {
                        ItemBatchOutputV1 itemBatchOutput = assetsApi.BatchCreateAssets(assetBatchInput);
                        AssetOutputV1 asset = assetsApi.GetAsset(itemBatchOutput.ItemUpdates[0].Item.Id);
                        this.assetTreeRootDataId = asset.DataId;
                        this.assetTreeRootName = asset.Name;
                        ItemIdListInputV1 itemIdListInput = new ItemIdListInputV1();
                        itemIdListInput.Items = new List<string> { itemBatchOutput.ItemUpdates[0].Item.Id };
                        treesApi.MoveNodesToRootOfTree(itemIdListInput);
                    }
                }
                if ((this?.ConditionConfigurations.Count ?? 0) == 0) {
                    log.Error("The ConditionConfigurations list must have at least one entry.");
                }
                if (this.ConditionConfigurations.GroupBy(x => x.ConditionName).Where(x => x.Count() > 1).Count() > 0) {
                    log.Error("Duplicate ConditionName found in ConditionConfigurations.");
                    return false;
                }
                foreach (ConditionConfigurationV1 conditionConfig in this.ConditionConfigurations) {
                    if (conditionConfig.CapsuleStartField != null && conditionConfig.CapsuleEndField != null &&
                        conditionConfig.CapsuleDurationField != null) {
                        throw new ArgumentException($"Bad ConditionConfiguration for ConditionName {conditionConfig.ConditionName};" +
                            $" is not valid; all three of CapsuleStartField, CapsuleEndField, and CapsuleDurationField cannot be " +
                            $"specified at once.");
                    }
                    if (conditionConfig.CapsuleStartField == null && conditionConfig.CapsuleEndField == null) {
                        throw new ArgumentException($"At least one of CapsuleStartField and CapsuleEndField must be specified, " +
                            $"which is not the case for ConditionConfiguration with ConditionName {conditionConfig.ConditionName}");
                    } else {
                        if ((conditionConfig.CapsuleStartField == null || conditionConfig.CapsuleEndField == null) &&
                            conditionConfig.CapsuleDurationField != null && conditionConfig.DefaultDuration != null) {
                            log.Error("CapsuleDurationField and DefaultDuration cannot both specified." +
                            $"which is not the case for ConditionConfiguration with ConditionName {conditionConfig.ConditionName}");
                        }
                    }
                    if (conditionConfig.DefaultDuration != null) {
                        TimeSpan testDuration;
                        if (TimeSpan.TryParse(conditionConfig.DefaultDuration, out testDuration) == false) {
                            throw new ArgumentException($"For ConditionConfiguration with ConditionName " +
                                $"{conditionConfig.ConditionName}, DefaultDuration {conditionConfig.DefaultDuration} could not " +
                                $"be parsed to a C# TimeSpan.");
                        }
                    }

                    List<PathTransformation> transformations = conditionConfig.PathTransformations?
                        .Take(conditionConfig.PathTransformations.Count - 1).ToList()
                        ?? new List<PathTransformation>();
                    foreach (PathTransformation transformation in transformations) {
                        if (transformation.Output == null || (transformation.FieldCaptures?.Count ?? 0) == 0) {
                            throw new ArgumentException("Missing Output or FieldCaptures for PathTransformation. Check config file.");
                        }
                        foreach (string fieldName in transformation.FieldCaptures?.Keys) {
                            if (transformation.Output.Contains($"${{{fieldName}}}") == false) {
                                log.Error($"For each FieldCaptureName, FieldCaptureRegex pair in the FieldCaptures dictionary" +
                                    $" of a PathTransformation, the Output must contain a string like ${{FieldCaptureNameHere}}.  " +
                                    $"This is not the case for a PathTransformation with Output {transformation.Output}.");
                                return false;
                            }
                        }
                    }

                    PathTransformation conditionNameTransformation = conditionConfig.PathTransformations.Last();
                    if (conditionNameTransformation != null) {
                        if (conditionNameTransformation.Output != null) {
                            throw new ArgumentException("The last item in the PathTranformations list should have a null Output field; " +
                                $"if no transformations are required, leave the FieldCaptures Dictionary empty");
                        }
                        foreach (string fieldName in conditionNameTransformation.FieldCaptures.Keys) {
                            if (conditionConfig.ConditionName.Contains($"${{{fieldName}}}") == false) {
                                log.Error($"For the last transformation in the PathTransformations list, each" +
                                    $" key of the FieldCaptures dictionary" +
                                    $" must appear in the ConditionName.  This is not the case for ConditionName {conditionConfig.ConditionName}.");
                                return false;
                            }
                        }
                    }
                }
                return true;
            } catch (Exception ex) {
                log.Error($"Reader initialization failed due to Exception: {ex.Message}", ex);
                return false;
            }
        }

        public override void ReadFile(string filename) {
            log.InfoFormat("Method ReadFile called for file {0}", filename);

            this.validateFileSizeLimit(log, this.ReaderConfiguration.MaxFileSizeInKB, filename);

            string assetPath = "";
            if (this.ReaderConfiguration.UseFilePathForHierarchy) {
                try {
                    if (filename.Contains(this.ReaderConfiguration.FilePathHierarchyRoot)) {
                        assetPath = filename.Substring(filename.LastIndexOf(this.ReaderConfiguration.FilePathHierarchyRoot));
                    } else {
                        throw new ArgumentException($"Filename {filename} must contain the " +
                            $"filePathHierarchyRoot, which is {this.ReaderConfiguration.FilePathHierarchyRoot}");
                    }
                    if (this.ReaderConfiguration.FilePathHierarchyIncludesFilename) {
                        if (assetPath.Contains(".")) {
                            assetPath = assetPath.Substring(0, assetPath.LastIndexOf('.'));
                        } else {
                            throw new ArgumentException($"When filePathHierarchyIncludesFilename is true, " +
                                $"all files must have an extension, which is not true for file: {filename}");
                        }
                    } else {
                        if (assetPath.Contains("\\")) {
                            assetPath = assetPath.Substring(0, assetPath.LastIndexOf('\\'));
                        }
                    }
                } catch (Exception ex) {
                    log.ErrorFormat("Failed to create asset path for file {0} due to exception: {1}",
                        filename, ex.Message);
                    throw ex;
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

            try {
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

                Dictionary<ConditionConfigurationV1, int> conditionStartIndices = new Dictionary<ConditionConfigurationV1, int>();
                Dictionary<ConditionConfigurationV1, int> conditionEndIndices = new Dictionary<ConditionConfigurationV1, int>();
                Dictionary<ConditionConfigurationV1, int> conditionDurationIndices = new Dictionary<ConditionConfigurationV1, int>();
                Dictionary<ConditionConfigurationV1, Dictionary<CapsulePropertyConfigV1, int>> conditionCapsulePropertyIndices =
                    new Dictionary<ConditionConfigurationV1, Dictionary<CapsulePropertyConfigV1, int>>();
                try {
                    foreach (ConditionConfigurationV1 conditionConfig in this.ConditionConfigurations) {
                        if (conditionConfig.CapsuleStartField != null) {
                            conditionStartIndices.Add(conditionConfig,
                                DirectoryWatchUtilities.GetHeaderIndex(conditionConfig.CapsuleStartField, headers));
                        }
                        if (conditionConfig.CapsuleEndField != null) {
                            conditionEndIndices.Add(conditionConfig,
                                DirectoryWatchUtilities.GetHeaderIndex(conditionConfig.CapsuleEndField, headers));
                        }
                        if (conditionConfig.CapsuleDurationField != null) {
                            conditionDurationIndices.Add(conditionConfig,
                                DirectoryWatchUtilities.GetHeaderIndex(conditionConfig.CapsuleDurationField, headers));
                        }
                        Dictionary<CapsulePropertyConfigV1, int> capsulePropertyIndices = new Dictionary<CapsulePropertyConfigV1, int>();
                        foreach (CapsulePropertyConfigV1 capsulePropertyConfig in
                            conditionConfig?.CapsuleProperties ?? new List<CapsulePropertyConfigV1>()) {
                            int propertyIndex = DirectoryWatchUtilities.GetHeaderIndex(capsulePropertyConfig.NameInFile, headers, capsulePropertyConfig.Required);
                            if (propertyIndex != -1) {
                                capsulePropertyIndices.Add(capsulePropertyConfig,
                                    DirectoryWatchUtilities.GetHeaderIndex(capsulePropertyConfig.NameInFile, headers));
                            }
                        }
                        conditionCapsulePropertyIndices.Add(conditionConfig, capsulePropertyIndices);
                    }
                } catch {
                    parser.Close();
                    parser.Dispose();
                    throw;
                }

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

                Dictionary<string, List<CapsuleInputV1>> seeqConditionData = new Dictionary<string, List<CapsuleInputV1>>();
                List<ConditionConfigurationV1> expandedConditionConfigurations = new List<ConditionConfigurationV1>();
                List<string> timestamps = new List<string>();
                string timestamp;
                int recordCounter = 0;
                while (parser.EndOfData == false) {
                    List<string> rowFields = parser.ReadFields().ToList();
                    foreach (ConditionConfigurationV1 conditionConfig in this.ConditionConfigurations) {
                        DateTime timestampAsDateTime;
                        string capsuleStartIsoString = null;
                        string capsuleEndIsoString = null;
                        string timeZoneFormatAffix = string.IsNullOrWhiteSpace(this.ReaderConfiguration?.TimeZone) ? "" : " zzz";
                        bool hasStartColumn = true;

                        // There is a bit of work to do to determine the correct timestamps for the row.  It all depends on
                        // which of the start column, end column, and duration column are specified in the config file and
                        // whether (if only a start or end column but not both were specified) a default duration was defined.
                        // No conflicting configurations should be allowed; for example, if start and end column are specified,
                        // no default duration or duration column should be permitted.

                        if (conditionStartIndices.ContainsKey(conditionConfig)) {
                            // Try parsing the timestamp without the timezone to confirm it is actually conformant to the user's configured timestampFormat.
                            // We append the timeZoneFormatAffix (if applicable), and if the parse is successful, we convert the DateTime to an ISO8601 string
                            string capsuleStartFromFile = rowFields[conditionStartIndices[conditionConfig]];
                            timestamp = string.IsNullOrWhiteSpace(this.ReaderConfiguration.TimeZone) ?
                                capsuleStartFromFile :
                                string.Format("{0}{1}", capsuleStartFromFile, this.ReaderConfiguration.TimeZone);

                            if (DateTime.TryParseExact(timestamp, this.ReaderConfiguration.TimestampFormat +
                                timeZoneFormatAffix, null, System.Globalization.DateTimeStyles.None, out timestampAsDateTime)) {
                                if (timestampAsDateTime.Kind == DateTimeKind.Unspecified) {
                                    log.Warn($"Timestamp {timestamp} was parsed as a DateTime with Unspecified DateTimeKind.");
                                    capsuleStartIsoString = timestampAsDateTime.ToString("o") + "Z";
                                } else {
                                    capsuleStartIsoString = timestampAsDateTime.ToString("o");
                                }
                            } else {
                                parser.Close();
                                parser.Dispose();
                                throw new Exception($"Timestamp {capsuleStartFromFile} joined from columns specified by" +
                                    $" CapsuleEndField for Condition {conditionConfig.ConditionName} did not conform to format" +
                                    $" {this.ReaderConfiguration.TimestampFormat} " +
                                    $"in file {filename}");
                            }
                        } else {
                            hasStartColumn = false;
                        }

                        if (conditionEndIndices.ContainsKey(conditionConfig)) {
                            // Try parsing the timestamp without the timezone to confirm it is actually conformant to the user's configured timestampFormat.
                            // We append the timeZoneFormatAffix (if applicable), and if the parse is successful, we convert the DateTime to an ISO8601 string
                            string capsuleEndFromFile = rowFields[conditionEndIndices[conditionConfig]];
                            timestamp = string.IsNullOrWhiteSpace(this.ReaderConfiguration.TimeZone) ?
                                capsuleEndFromFile :
                                string.Format("{0}{1}", capsuleEndFromFile, this.ReaderConfiguration.TimeZone);
                            if (DateTime.TryParseExact(timestamp, this.ReaderConfiguration.TimestampFormat +
                                timeZoneFormatAffix, null, System.Globalization.DateTimeStyles.None, out timestampAsDateTime)) {
                                if (timestampAsDateTime.Kind == DateTimeKind.Unspecified) {
                                    log.Warn($"Timestamp {timestamp} was parsed as a DateTime with Unspecified DateTimeKind.");
                                    capsuleEndIsoString = timestampAsDateTime.ToString("o") + "Z";
                                } else {
                                    capsuleEndIsoString = timestampAsDateTime.ToString("o");
                                }
                            } else {
                                parser.Close();
                                parser.Dispose();
                                throw new Exception($"Timestamp {capsuleEndFromFile} joined from columns specified by" +
                                    $" CapsuleEndField for Condition {conditionConfig.ConditionName} did not conform to format" +
                                    $" {this.ReaderConfiguration.TimestampFormat} " +
                                    $"in file {filename}");
                            }
                        }

                        if (conditionConfig.DefaultDuration != null || conditionDurationIndices.ContainsKey(conditionConfig)) {
                            TimeSpan duration = TimeSpan.Parse(conditionConfig?.DefaultDuration ??
                                rowFields[conditionDurationIndices[conditionConfig]]);
                            if (hasStartColumn) {
                                capsuleEndIsoString = DateTime.Parse(capsuleStartIsoString).Add(duration).ToString("o");
                            } else {
                                capsuleStartIsoString = DateTime.Parse(capsuleEndIsoString).Subtract(duration).ToString("o");
                            }
                        }

                        List<ScalarPropertyV1> capsuleProperties = new List<ScalarPropertyV1>();
                        foreach (CapsulePropertyConfigV1 capsulePropertyConfig in
                            conditionConfig?.CapsuleProperties ?? new List<CapsulePropertyConfigV1>()) {
                            if (conditionCapsulePropertyIndices[conditionConfig].ContainsKey(capsulePropertyConfig)) {
                                ScalarPropertyV1 property = new ScalarPropertyV1 {
                                    Name = capsulePropertyConfig.NameInSeeq,
                                    UnitOfMeasure = capsulePropertyConfig.UnitOfMeasure,
                                    Value = rowFields[conditionCapsulePropertyIndices[conditionConfig][capsulePropertyConfig]]
                                };
                                capsuleProperties.Add(property);
                            }
                        }
                        string seeqConditionPath = "";
                        if (this.ReaderConfiguration.UseFilePathForHierarchy) {
                            seeqConditionPath = assetPath + pathSeparator;
                        }

                        // If no transformations exist, just append the ConditionName to the path.
                        if ((conditionConfig?.PathTransformations.Count ?? 0) == 0) {
                            seeqConditionPath += conditionConfig.ConditionName;
                        }

                        // Each transformation except the last is considered an extension of the path already defined;
                        // the last transformation is considered a transformation of the Condition Name itself.
                        foreach (PathTransformation transformation in
                            conditionConfig?.PathTransformations ?? new List<PathTransformation>()) {
                            // The prechecks in Initialize() should ensure only the last transformation has null Output.
                            string output = transformation.Output ?? conditionConfig.ConditionName;
                            foreach (string fieldName in transformation.FieldCaptures.Keys) {
                                string fieldValue = rowFields[DirectoryWatchUtilities.GetHeaderIndex(fieldName, headers, true)];
                                Regex regex = new Regex(transformation.FieldCaptures[fieldName]);
                                MatchCollection matches = regex.Matches(fieldValue);
                                if (matches.Count == 1 && matches[0].Groups.Count == 2) {
                                    // Note that the existence of a capture variable in the Output was confirmed in Initialize().
                                    output = output.Replace($"${{{fieldName}}}", matches[0].Groups[1].Value);
                                } else {
                                    throw new System.IO.IOException("Could not transform path using transformation with output " +
                                        $"{transformation.Output} for line {parser.LineNumber} of file {filename}");
                                }
                            }
                            seeqConditionPath += output + (transformation.Output != null ? pathSeparator : "");
                        }

                        if (seeqConditionData.ContainsKey(seeqConditionPath) == false) {
                            seeqConditionData[seeqConditionPath] = new List<CapsuleInputV1>();
                            string conditionName =
                                seeqConditionPath.Split(new string[] { pathSeparator }, StringSplitOptions.None).Last();
                            if (!expandedConditionConfigurations.Exists(x => x.ConditionName == conditionName)) {
                                expandedConditionConfigurations.Add(new ConditionConfigurationV1 {
                                    ConditionName = conditionName,
                                    CapsuleDurationField = conditionConfig.CapsuleDurationField,
                                    CapsuleStartField = conditionConfig.CapsuleStartField,
                                    CapsuleEndField = conditionConfig.CapsuleEndField,
                                    CapsuleProperties = conditionConfig.CapsuleProperties,
                                    DefaultDuration = conditionConfig.DefaultDuration,
                                    MaximumDuration = conditionConfig.MaximumDuration,
                                    PathTransformations = conditionConfig.PathTransformations,
                                    Required = conditionConfig.Required
                                });
                            }
                        }
                        seeqConditionData[seeqConditionPath].Add(new CapsuleInputV1 {
                            Start = capsuleStartIsoString,
                            End = capsuleEndIsoString,
                            Properties = capsuleProperties
                        });
                    }

                    recordCounter++;
                    if (recordCounter % this.ReaderConfiguration.RecordsPerDataPacket == 0) {
                        log.Info($"Sending {this.ReaderConfiguration.RecordsPerDataPacket} rows of data to Seeq; recordCount is {recordCounter}");
                        DirectoryWatchConditionData data = new DirectoryWatchConditionData() {
                            SeeqConditionData = seeqConditionData,
                            AssetTreeRoot = new AssetOutputV1() {
                                DataId = this.assetTreeRootDataId,
                                Name = this.assetTreeRootName
                            },
                            Connection = this.Connection,
                            Filename = filename,
                            PathSeparator = this.pathSeparator,
                            ConditionConfigurations = expandedConditionConfigurations
                        };
                        DirectoryWatchUtilities.SendConditionData(data);
                        seeqConditionData.Clear();
                        expandedConditionConfigurations.Clear();
                    }
                }

                if (seeqConditionData.Count > 0) {
                    DirectoryWatchConditionData data = new DirectoryWatchConditionData() {
                        SeeqConditionData = seeqConditionData,
                        AssetTreeRoot = new AssetOutputV1() {
                            DataId = this.assetTreeRootDataId,
                            Name = this.assetTreeRootName
                        },
                        Connection = this.Connection,
                        Filename = filename,
                        PathSeparator = this.pathSeparator,
                        ConditionConfigurations = expandedConditionConfigurations
                    };
                    DirectoryWatchUtilities.SendConditionData(data);
                }

                log.InfoFormat("Completed reading all data from file {0}; sending data to Seeq database", filename);

                this.Connection.MetadataSync(SyncMode.Full);
            } finally {
                parser.Close();
                parser.Dispose();
            }
        }
    }
}