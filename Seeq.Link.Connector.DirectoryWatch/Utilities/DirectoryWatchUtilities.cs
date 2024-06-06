using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Seeq.Link.Connector.DirectoryWatch.Config;
using Seeq.Link.SDK.Interfaces;
using Seeq.Sdk.Api;
using Seeq.Sdk.Model;

namespace Seeq.Link.Connector.DirectoryWatch.Utilities {

    public class DirectoryWatchUtilities {
        private static readonly object lockObj = new object();

        protected static readonly log4net.ILog log = log4net.LogManager.GetLogger
            (System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        public static string GuidifyString(string inputString) {
            using (System.Security.Cryptography.MD5 md5 = System.Security.Cryptography.MD5.Create()) {
                byte[] hash = md5.ComputeHash(Encoding.Default.GetBytes(inputString));
                Guid result = new Guid(hash);
                return result.ToString();
            }
        }

        public static SampleInputV1 SeeqSample(string timestamp, string value) {
            long sampleValueLong;
            double sampleValueDouble;
            if (string.IsNullOrEmpty(value) == false) {
                if (long.TryParse(value, out sampleValueLong)) {
                    return new SampleInputV1 {
                        Key = timestamp,
                        Value = sampleValueLong
                    };
                } else if (double.TryParse(value, out sampleValueDouble)) {
                    return new SampleInputV1 {
                        Key = timestamp,
                        Value = sampleValueDouble
                    };
                } else {
                    return new SampleInputV1 {
                        Key = timestamp,
                        Value = value
                    };
                }
            } else {
                return null;
            }
        }

        /// <summary>
        /// Checks for existence of a match to the headerExpression among the fileHeaders.  If headerExpression
        /// is bracketed with forward slashes, it will be used as a regex pattern, otherwise an exact match is required.
        /// In either case, duplicates cause an ArgumentException to be thrown.  If no match is found, the method
        /// returns -1 if throwIfNoMatch is false or throws an ArgumentException if it is true.
        /// </summary>
        /// <param name="headerExpression"></param>
        /// <param name="fileHeaders"></param>
        /// <param name="throwIfNoMatch"></param>
        /// <returns></returns>
        public static int GetHeaderIndex(string headerExpression, List<string> fileHeaders, bool throwIfNoMatch = true) {
            List<int> headerIndices = new List<int>();
            if (headerExpression.StartsWith("/") && headerExpression.EndsWith("/") && headerExpression.Length > 2) {
                // this is a regex header
                Regex regex = new Regex(headerExpression.Substring(1, headerExpression.Length - 2));
                foreach (string fileHeader in fileHeaders) {
                    if (regex.IsMatch(fileHeader)) {
                        headerIndices.Add(fileHeaders.IndexOf(fileHeader));
                    }
                }
            } else {
                if (fileHeaders.Exists(x => x == headerExpression)) {
                    headerIndices.AddRange(fileHeaders.FindAll(x => x == headerExpression)
                                                             .Select(x => fileHeaders.IndexOf(x)));
                }
            }

            if (headerIndices.Count == 0) {
                if (throwIfNoMatch) {
                    throw new ArgumentException(string.Format("Header expression {0} passed to getHeaderIndex did not match any of the available headers in the file.", headerExpression));
                } else {
                    return -1;
                }
            } else if (headerIndices.Count > 1) {
                throw new ArgumentException(string.Format("Header expression {0} passed to getHeaderIndex matched more than one header in the file.", headerExpression));
            } else {
                return headerIndices[0];
            }
        }

        public static DatasourceOutputV1 GetDirectoryWatchDatasource(DirectoryWatchConnection connection) {
            IDatasourcesApi datasourcesApi = connection.AgentService.ApiProvider.CreateDatasourcesApi();
            string directoryWatch = "DirectoryWatch";
            DatasourceOutputListV1 datasourceOutputList = datasourcesApi.GetDatasources(directoryWatch, directoryWatch, 0, 2, false);
            if (datasourceOutputList.Datasources.Count != 1) {
                throw new ArgumentException("Cannot find DirectoryWatch datasource; this should have been created on connector initialization.");
            } else {
                return datasourceOutputList.Datasources[0];
            }
        }

        /// <summary>
        /// Creates or updates the root asset.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="rootAsset"></param>
        /// <returns>The AssetOutputV1 for the root asset</returns>
        public static AssetOutputV1 SetRootAsset(DirectoryWatchConnection connection, string assetName, string scopedTo = null) {
            lock (lockObj) {
                string datasourceId = GetDirectoryWatchDatasource(connection).Id;
                IAssetsApi assetsApi = connection.AgentService.ApiProvider.CreateAssetsApi();
                ITreesApi treesApi = connection.AgentService.ApiProvider.CreateTreesApi();

                string dataId = GuidifyString(assetName);

                AssetBatchInputV1 assetBatchInput = new AssetBatchInputV1 {
                    HostId = datasourceId,
                    Assets = new List<PutAssetInputV1> { new PutAssetInputV1 {
                    Name = assetName,
                    HostId = datasourceId,
                    DataId = dataId,
                    ScopedTo = scopedTo,
                    Properties = new List<ScalarPropertyV1> {
                        new ScalarPropertyV1 { Name = "ProjectRoot",
                                                Value = true } }
                } }
                };
                ItemUpdateOutputV1 itemUpdateOutput = assetsApi.BatchCreateAssets(assetBatchInput).ItemUpdates[0];
                string rootAssetId = itemUpdateOutput.Item.Id;
                ItemIdListInputV1 itemIdListInput = new ItemIdListInputV1();
                itemIdListInput.Items = new List<string> { itemUpdateOutput.Item.Id };
                treesApi.MoveNodesToRootOfTree(itemIdListInput);
                return new AssetOutputV1 {
                    Name = itemUpdateOutput.Item.Name,
                    DataId = itemUpdateOutput.DataId,
                    DatasourceClass = itemUpdateOutput.DatasourceClass,
                    DatasourceId = itemUpdateOutput.DatasourceId,
                    Id = itemUpdateOutput.Item.Id,
                    ScopedTo = scopedTo,
                    IsArchived = itemUpdateOutput.Item.IsArchived
                };
            }
        }

        public static bool SendData(DirectoryWatchData data, bool skipBadSamples = false, bool skipNullValueSamples = false, bool noTree = false) {
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

            bool success = true;

            try {
                IItemsApi itemsApi = data.Connection.AgentService.ApiProvider.CreateItemsApi();
                ISignalsApi signalsApi = data.Connection.AgentService.ApiProvider.CreateSignalsApi();
                // Since DataID is ignored by appserver for storedInSeeq signals, we use description as a proxy for this
                // for now.  This means the current version ignores the user-specified Description in the SignalConfigurations

                IDatasourcesApi datasourcesApi = data.Connection.AgentService.ApiProvider.CreateDatasourcesApi();
                string syncToken = DateTime.UtcNow.ToString("o");

                DatasourceOutputV1 datasourceOutput = GetDirectoryWatchDatasource(data.Connection);
                string datasourceId = datasourceOutput.Id;
                IAssetsApi assetsApi = data.Connection.AgentService.ApiProvider.CreateAssetsApi();
                ITreesApi treesApi = data.Connection.AgentService.ApiProvider.CreateTreesApi();

                AssetBatchInputV1 assetBatchInput = new AssetBatchInputV1();
                assetBatchInput.HostId = datasourceId;
                assetBatchInput.Assets = new List<PutAssetInputV1>();

                PutSignalsInputV1 putSignalsInput = new PutSignalsInputV1();
                putSignalsInput.Signals = new List<SignalWithIdInputV1>();

                AssetTreeBatchInputV1 assetTreeBatchInput = new AssetTreeBatchInputV1();
                assetTreeBatchInput.ParentHostId = datasourceId;
                assetTreeBatchInput.ChildHostId = datasourceId;
                assetTreeBatchInput.Relationships = new List<AssetTreeSingleInputV1>();

                if (data.ScopedTo != null) {
                    Guid testGuid;
                    if (!Guid.TryParse(data.ScopedTo, out testGuid)) {
                        throw new ArgumentException($"ScopedTo parameter {data.ScopedTo} of DirectoryWatchData is not a valid GUID; " +
                            "please check the reader configuration and try again.");
                    }
                }

                foreach (string path in data.SeeqSignalData.Keys) {
                    List<string> nodes = path.Split(new[] { data.PathSeparator }, StringSplitOptions.None).ToList();
                    if (nodes.Count != path.Split(new[] { data.PathSeparator }, StringSplitOptions.RemoveEmptyEntries).ToList().Count) {
                        log.Error($"Failed to create tree from path {path} due to repeated separator: {data.PathSeparator}");
                        success = false;
                        continue;
                    }

                    string partialPathGuid = null;
                    if (!noTree) {
                        string partialPath = nodes[0]; // The root of the tree must already be defined!
                        partialPathGuid = GuidifyString(partialPath);
                        foreach (string node in nodes.Skip(1).Take(nodes.Count - 2)) {
                            string parentPathGuid = partialPathGuid;
                            partialPath += data.PathSeparator + node;
                            partialPathGuid = GuidifyString(partialPath);

                            if (assetBatchInput.Assets.Exists(x => x.DataId == partialPathGuid) == false) {
                                assetBatchInput.Assets.Add(
                                    new PutAssetInputV1 { Name = node, HostId = datasourceId, DataId = partialPathGuid, SyncToken = syncToken, ScopedTo = data.ScopedTo });
                                assetTreeBatchInput.Relationships.Add(
                                    new AssetTreeSingleInputV1 { ParentDataId = parentPathGuid, ChildDataId = partialPathGuid });
                            }
                        }
                    }

                    string seeqSignalName = nodes[nodes.Count - 1];
                    string pathGuid = GuidifyString(path);
                    SignalWithIdInputV1 signalWithIdInput = new SignalWithIdInputV1 {
                        Name = seeqSignalName,
                        DatasourceClass = datasourceOutput.DatasourceClass,
                        DatasourceId = datasourceOutput.DatasourceId,
                        SyncToken = syncToken,
                        DataId = pathGuid,
                        ScopedTo = data.ScopedTo,
                        Description = path + " (" + data.Connection.DatasourceId + " signal)",
                        InterpolationMethod = data.SignalConfigurations.Find(x => x.NameInSeeq == seeqSignalName).InterpolationType,
                        MaximumInterpolation = data.SignalConfigurations.Find(x => x.NameInSeeq == seeqSignalName).MaximumInterpolation,
                        ValueUnitOfMeasure = data.SignalConfigurations.Find(x => x.NameInSeeq == seeqSignalName).Uom
                    };
                    putSignalsInput.Signals.Add(signalWithIdInput);

                    if (!noTree) {
                        assetTreeBatchInput.Relationships.Add(
                            new AssetTreeSingleInputV1 { ParentDataId = partialPathGuid, ChildDataId = pathGuid });
                    }
                }

                bool moreToDo;
                int offset;
                int limit;

                if (!noTree) {
                    moreToDo = true;
                    offset = 0;
                    limit = 1000;
                    while (moreToDo) {
                        int assetCount = offset + limit < assetBatchInput.Assets.Count ? limit : assetBatchInput.Assets.Count - offset;
                        AssetBatchInputV1 assetBatchInputPartial = new AssetBatchInputV1 {
                            HostId = assetBatchInput.HostId,
                            Assets = assetBatchInput.Assets.GetRange(offset, assetCount)
                        };
                        ItemBatchOutputV1 assetBatchOutput = assetsApi.BatchCreateAssets(assetBatchInputPartial);
                        moreToDo = (assetCount == limit);
                        offset += limit;
                    }
                }

                ItemBatchOutputV1 itemBatchOutput = new ItemBatchOutputV1();
                itemBatchOutput.ItemUpdates = new List<ItemUpdateOutputV1>();
                moreToDo = true;
                offset = 0;
                limit = 1000;
                while (moreToDo) {
                    int seriesCount = offset + limit < putSignalsInput.Signals.Count ?
                        limit : putSignalsInput.Signals.Count - offset;
                    PutSignalsInputV1 putSignalsInputPartial = new PutSignalsInputV1 {
                        Signals = putSignalsInput.Signals.GetRange(offset, seriesCount)
                    };
                    ItemBatchOutputV1 itemBatchOutputPartial = signalsApi.PutSignals(putSignalsInputPartial);
                    itemBatchOutput.ItemUpdates.AddRange(itemBatchOutputPartial.ItemUpdates);
                    offset += limit;
                    moreToDo = (seriesCount == limit);
                }

                if (!noTree) {
                    moreToDo = true;
                    offset = 0;
                    limit = 1000;
                    while (moreToDo) {
                        int relationshipCount = offset + limit < assetTreeBatchInput.Relationships.Count ?
                            limit : assetTreeBatchInput.Relationships.Count - offset;
                        AssetTreeBatchInputV1 assetTreeBatchInputPartial = new AssetTreeBatchInputV1 {
                            ParentHostId = assetTreeBatchInput.ParentHostId,
                            ChildHostId = assetTreeBatchInput.ChildHostId,
                            Relationships = assetTreeBatchInput.Relationships.GetRange(offset, relationshipCount)
                        };
                        treesApi.BatchMoveNodesToParents(assetTreeBatchInputPartial);
                        offset += limit;
                        moreToDo = (relationshipCount == limit);
                    }
                }

                foreach (string seeqSignalPath in data.SeeqSignalData.Keys) {
                    string seeqSignalName = seeqSignalPath.Substring(seeqSignalPath.LastIndexOf(data.PathSeparator) + data.PathSeparator.Length);
                    string seeqSignalId;
                    bool containsString = false;
                    bool containsNumeric = false;

                    seeqSignalId = itemBatchOutput.ItemUpdates.Find(x =>
                        x.DataId == GuidifyString(seeqSignalPath)).Item.Id;

                    string firstCachedTimestamp = null;
                    string lastCachedTimestamp = null;
                    string datastoreStatus = null;
                    try {
                        firstCachedTimestamp = itemsApi.GetProperty(seeqSignalId, "FirstCachedTimestamp").Value;
                    } catch {
                        firstCachedTimestamp = "2200-01-01T00:00:00Z";
                    }
                    try {
                        lastCachedTimestamp = itemsApi.GetProperty(seeqSignalId, "LastCachedTimestamp").Value;
                    } catch {
                        lastCachedTimestamp = "1970-01-02T00:00:00Z";
                    }
                    try {
                        datastoreStatus = itemsApi.GetProperty(seeqSignalId, "DatastoreStatus").Value;
                    } catch {
                        datastoreStatus = "Active";
                    }

                    if (datastoreStatus == "Sealed") {
                        log.Warn($"A Sealed signal with description {seeqSignalPath} was found in file " +
                            $"{data.Filename} and will be skipped");
                        continue;
                    }
                    if (datastoreStatus == "Reset") {
                        firstCachedTimestamp = "2200-01-02T00:00:00Z";
                        lastCachedTimestamp = "1970-01-02T00:00:00Z";
                    }
                    SamplesOverwriteInputV1 samplesInput = new SamplesOverwriteInputV1();
                    samplesInput.Samples = new List<SampleInputV1>();
                    string newLastCachedTimestamp = null;
                    SampleInputV1 firstSample = data.SeeqSignalData[seeqSignalPath] != null
                        && data.SeeqSignalData[seeqSignalPath].Count > 0 ? data.SeeqSignalData[seeqSignalPath][0] : null;
                    if (firstSample != null && DateTime.Parse(firstSample.Key.ToString()) < DateTime.Parse(firstCachedTimestamp)) {
                        itemsApi.SetProperty(seeqSignalId, "FirstCachedTimestamp",
                            new PropertyInputV1 { Value = firstSample.Key.ToString(), UnitOfMeasure = "string" });
                    }
                    // Test if any samples are invalid and what kind of signal this is (string or numeric)
                    foreach (SampleInputV1 seriesSample in data.SeeqSignalData[seeqSignalPath]) {
                        if (seriesSample.Value == null) {
                            if (!skipNullValueSamples) {
                                samplesInput.Samples.Add(seriesSample);
                                // update the last timestamp if this sample is now the latest one.
                                if (DateTime.Parse(seriesSample.Key.ToString()) >= DateTime.Parse(lastCachedTimestamp)) {
                                    newLastCachedTimestamp = seriesSample.Key.ToString();
                                }
                            } else {
                                log.Warn($"Found null value sample for seeqSignalPath {seeqSignalPath} and skipNullValueSamples was false; cannot continue posting data to this Signal in SendData call.  Note that this will not fail the import, since other Signals may be fine.");
                            }
                            continue;
                        }

                        // if there are strings, note that
                        if (seriesSample.Value?.GetType() == typeof(string)) {
                            containsString = true;
                        } else { // if there are numbers, note that
                            containsNumeric = true;
                        }

                        // convert samples to doubles as possible
                        double sampleValueAsDouble;
                        DateTime sampleKeyAsDateTime;
                        bool sampleOK = double.TryParse(seriesSample.Value.ToString(), out sampleValueAsDouble) ||
                            seriesSample.Value.GetType() == typeof(string);
                        sampleOK = sampleOK && DateTime.TryParse(seriesSample.Key.ToString(), out sampleKeyAsDateTime);
                        if (sampleOK) {
                            samplesInput.Samples.Add(seriesSample);
                        } else if (skipBadSamples) { // if we aren't supposed to skip bad samples, error out
                            continue;
                        } else {
                            throw new ArgumentException($"Found bad sample in data for Signal with path {seeqSignalPath}" +
                                " and skipBadSamples was false.");
                        }
                        // update the last timestamp if this sample is now the latest one.
                        if (DateTime.Parse(seriesSample.Key.ToString()) >= DateTime.Parse(lastCachedTimestamp)) {
                            newLastCachedTimestamp = seriesSample.Key.ToString();
                        }
                    }
                    // If the signal has strings and numbers, assume it's a numeric signal and remove all the strings
                    if (containsNumeric && containsString) {
                        log.Error($"Signal {seeqSignalId} contains both strings and numbers; SendData call failed.");
                        return false;
                    }

                    if (samplesInput.Samples.Count > 0) {
                        signalsApi.PutSamples(seeqSignalId, samplesInput);
                    }
                    if (newLastCachedTimestamp != null) {
                        itemsApi.SetProperty(seeqSignalId, "LastCachedTimestamp",
                            new PropertyInputV1 { Value = newLastCachedTimestamp, UnitOfMeasure = "string" });
                    }
                    itemsApi.SetProperty(seeqSignalId, "DatastoreStatus",
                            new PropertyInputV1 { Value = datastoreStatus, UnitOfMeasure = "string" });
                }
            } catch (Exception ex) {
                log.Error($"Call to SendData for file {data.Filename} failed due to exception: {ex.Message}", ex);
                success = false;
            }
            return success;
        }

        /// <summary>
        /// This method posts the data in the DirectoryWatchConditionData to Seeq if possible.  The boolean
        /// throwExceptionOnInvalidTimestamps is optional and false by default.  If true, the read operation
        /// will stop with a thrown exception when the first CapsuleV1 in the SeeqConditionData list with an
        /// invalid Start or End time is encountered.  The boolean ignoreUnspecifiedProperties is optional
        /// and true by default.  If false, a CapsuleV1 in the SeeqConditionDate list will cause an exception
        /// to be thrown and the read operation will stop.
        /// </summary>
        /// <param name="data"></param>
        /// <param name="throwExceptionOnInvalidTimestamps"></param>
        /// <param name="ignoreUnspecifiedProperties"></param>
        /// <returns></returns>
        public static bool SendConditionData(DirectoryWatchConditionData data,
            bool throwExceptionOnInvalidTimestamps = false,
            bool ignoreUnspecifiedProperties = true) {
            bool success = true;

            try {
                IItemsApi itemsApi = data.Connection.AgentService.ApiProvider.CreateItemsApi();
                IConditionsApi conditionsApi = data.Connection.AgentService.ApiProvider.CreateConditionsApi();
                // Since DataID is ignored by appserver for storedInSeeq signals, we use description as a proxy for this
                // for now.  This means the current version ignores the user-specified Description in the SignalConfigurations

                IDatasourcesApi datasourcesApi = data.Connection.AgentService.ApiProvider.CreateDatasourcesApi();
                DatasourceOutputV1 datasourceOutput = GetDirectoryWatchDatasource(data.Connection);
                string datasourceId = datasourceOutput.Id;
                IAssetsApi assetsApi = data.Connection.AgentService.ApiProvider.CreateAssetsApi();
                ITreesApi treesApi = data.Connection.AgentService.ApiProvider.CreateTreesApi();

                AssetBatchInputV1 assetBatchInput = new AssetBatchInputV1();
                assetBatchInput.HostId = datasourceId;
                assetBatchInput.Assets = new List<PutAssetInputV1>();

                ConditionBatchInputV1 conditionBatchInput = new ConditionBatchInputV1();
                conditionBatchInput.Conditions = new List<ConditionUpdateInputV1>();

                AssetTreeBatchInputV1 assetTreeBatchInput = new AssetTreeBatchInputV1();
                assetTreeBatchInput.ParentHostId = datasourceId;
                assetTreeBatchInput.ChildHostId = datasourceId;
                assetTreeBatchInput.Relationships = new List<AssetTreeSingleInputV1>();

                Dictionary<string, string> dataIdMappings = new Dictionary<string, string>();
                foreach (string path in data.SeeqConditionData.Keys) {
                    List<string> nodes = path.Split(new[] { data.PathSeparator }, StringSplitOptions.None).ToList();
                    if (nodes.Count != path.Split(new[] { data.PathSeparator }, StringSplitOptions.RemoveEmptyEntries).ToList().Count) {
                        log.Error($"Failed to create tree from path {path} due to repeated separator: {data.PathSeparator}");
                        success = false;
                        continue;
                    }

                    string partialPath = nodes[0]; // The root of the tree must already be defined!
                    string partialPathGuid = data.AssetTreeRoot.DataId;
                    dataIdMappings[GuidifyString(data.AssetTreeRoot.Name)]
                        = data.AssetTreeRoot.DataId;
                    foreach (string node in nodes.Skip(1).Take(nodes.Count - 2)) {
                        string parentPathGuid = partialPathGuid;
                        partialPath += data.PathSeparator + node;
                        partialPathGuid = GuidifyString(partialPath);

                        if (assetBatchInput.Assets.Exists(x => x.DataId == partialPathGuid) == false) {
                            dataIdMappings[partialPathGuid] = null;
                            assetBatchInput.Assets.Add(
                                new PutAssetInputV1 { Name = node, HostId = datasourceId, DataId = partialPathGuid });
                            assetTreeBatchInput.Relationships.Add(
                                new AssetTreeSingleInputV1 { ParentDataId = parentPathGuid, ChildDataId = partialPathGuid });
                        }
                    }
                    string seeqConditionName = nodes[nodes.Count - 1];
                    string pathGuid = GuidifyString(path);
                    dataIdMappings[pathGuid] = null;
                    var conditionInput = new ConditionUpdateInputV1 {
                        Name = seeqConditionName,
                        DatasourceClass = datasourceOutput.DatasourceClass,
                        DatasourceId = datasourceOutput.DatasourceId,
                        DataId = pathGuid,
                        Description = path + " (" + data.Connection.DatasourceId + " Condition)",
                        MaximumDuration = data.ConditionConfigurations.Find(x => x.ConditionName == seeqConditionName).MaximumDuration,
                    };
                    conditionBatchInput.Conditions.Add(conditionInput);

                    assetTreeBatchInput.Relationships.Add(
                        new AssetTreeSingleInputV1 { ParentDataId = partialPathGuid, ChildDataId = pathGuid });
                }

                bool moreToDo = true;
                int offset = 0;
                int limit = 1000;
                while (moreToDo) {
                    int assetCount = offset + limit < assetBatchInput.Assets.Count ? limit : assetBatchInput.Assets.Count - offset;
                    AssetBatchInputV1 assetBatchInputPartial = new AssetBatchInputV1 {
                        HostId = assetBatchInput.HostId,
                        Assets = assetBatchInput.Assets.GetRange(offset, assetCount)
                    };
                    ItemBatchOutputV1 assetBatchOutput = assetsApi.BatchCreateAssets(assetBatchInputPartial);
                    moreToDo = (assetCount == limit);
                    offset += limit;
                }

                ItemBatchOutputV1 itemBatchOutput = new ItemBatchOutputV1();
                itemBatchOutput.ItemUpdates = new List<ItemUpdateOutputV1>();
                moreToDo = true;
                offset = 0;
                limit = 1000;
                while (moreToDo) {
                    int conditionCount = offset + limit < conditionBatchInput.Conditions.Count ?
                        limit : conditionBatchInput.Conditions.Count - offset;
                    ConditionBatchInputV1 conditionBatchInputPartial = new ConditionBatchInputV1 {
                        Conditions = conditionBatchInput.Conditions.GetRange(offset, conditionCount)
                    };
                    ItemBatchOutputV1 itemBatchOutputPartial = conditionsApi.PutConditions(conditionBatchInputPartial);
                    itemBatchOutput.ItemUpdates.AddRange(itemBatchOutputPartial.ItemUpdates);
                    offset += limit;
                    moreToDo = (conditionCount == limit);
                }
                moreToDo = true;
                offset = 0;
                limit = 1000;
                while (moreToDo) {
                    int relationshipCount = offset + limit < assetTreeBatchInput.Relationships.Count ?
                        limit : assetTreeBatchInput.Relationships.Count - offset;
                    AssetTreeBatchInputV1 assetTreeBatchInputPartial = new AssetTreeBatchInputV1 {
                        ParentHostId = assetTreeBatchInput.ParentHostId,
                        ChildHostId = assetTreeBatchInput.ChildHostId,
                        Relationships = assetTreeBatchInput.Relationships.GetRange(offset, relationshipCount)
                    };
                    treesApi.BatchMoveNodesToParents(assetTreeBatchInputPartial);
                    offset += limit;
                    moreToDo = (relationshipCount == limit);
                }

                // Since DataID is ignored by appserver for storedInSeeq signals, we use description as a proxy for this
                // for now.  This means the current version ignores the user-specified Description in the SignalConfigurations
                foreach (string seeqConditionPath in data.SeeqConditionData.Keys) {
                    string seeqConditionName = seeqConditionPath.Substring(seeqConditionPath.LastIndexOf(data.PathSeparator) + data.PathSeparator.Length);
                    string seeqConditionId;

                    try {
                        seeqConditionId = itemBatchOutput.ItemUpdates.Find(x =>
                            x.DataId == GuidifyString(seeqConditionPath)).Item.Id;
                    } catch (Exception) {
                        log.Error($"Could not find matching Condition for path {seeqConditionPath} while reading {data.Filename}");
                        throw;
                    }

                    string firstCachedTimestamp = null;
                    string lastCachedTimestamp = null;
                    string datastoreStatus = null;
                    try {
                        firstCachedTimestamp = itemsApi.GetProperty(seeqConditionId, "FirstCachedTimestamp").Value;
                    } catch {
                        firstCachedTimestamp = "2200-01-01T00:00:00Z";
                    }
                    try {
                        lastCachedTimestamp = itemsApi.GetProperty(seeqConditionId, "LastCachedTimestamp").Value;
                    } catch {
                        lastCachedTimestamp = "1970-01-02T00:00:00Z";
                    }
                    try {
                        datastoreStatus = itemsApi.GetProperty(seeqConditionId, "DatastoreStatus").Value;
                    } catch {
                        datastoreStatus = "Active";
                    }

                    if (datastoreStatus == "Sealed") {
                        log.Warn($"A Sealed signal with description {seeqConditionPath} was found in file " +
                            $"{data.Filename} and will be skipped");
                        continue;
                    }
                    if (datastoreStatus == "Reset") {
                        firstCachedTimestamp = "2200-01-02T00:00:00Z";
                        lastCachedTimestamp = "1970-01-02T00:00:00Z";
                    }
                    CapsulesInputV1 capsulesInput = new CapsulesInputV1();
                    capsulesInput.Capsules = new List<CapsuleInputV1>();
                    string newLastCachedTimestamp = null;
                    CapsuleInputV1 firstCapsule = data.SeeqConditionData[seeqConditionPath] != null &&
                        data.SeeqConditionData[seeqConditionPath].Count > 0 ? data.SeeqConditionData[seeqConditionPath][0] : null;
                    if (firstCapsule != null && DateTime.Parse(firstCapsule.Start.ToString()) < DateTime.Parse(firstCachedTimestamp)) {
                        itemsApi.SetProperty(seeqConditionId, "FirstCachedTimestamp",
                            new PropertyInputV1 { Value = firstCapsule.Start.ToString(), UnitOfMeasure = "string" });
                    }
                    foreach (CapsuleInputV1 capsule in data.SeeqConditionData[seeqConditionPath]) {
                        // confirm that capsule start/end are valid; depending on skip settings, either throw an
                        // exception or continue to next capsule
                        if (!(IsValidSeeqTime(capsule.Start, false) && IsValidSeeqTime(capsule.End, false))) {
                            if (throwExceptionOnInvalidTimestamps) {
                                throw new ArgumentException($"An invalid timestamp was encountered on capsule with Start {capsule.Start}" +
                                    $", End {capsule.End} for Condition with path {seeqConditionPath}" +
                                    ", and throwExceptionOnInvalidTimestamps was set to true.");
                            } else {
                                log.Warn($"Skipping capsule due to invalid start/end times {capsule.Start}, {capsule.End}");
                                continue;
                            }
                        }
                        ConditionConfigurationV1 config =
                            data.ConditionConfigurations.Find(x => x.ConditionName == seeqConditionName);
                        int capsulePropertyCount = 0;
                        foreach (CapsulePropertyConfigV1 capsulePropertyConfig in
                            config?.CapsuleProperties ?? new List<CapsulePropertyConfigV1>()) {
                            ScalarPropertyV1 capsuleProperty = capsule.Properties.Find(x => x.Name == capsulePropertyConfig.NameInSeeq);
                            if (capsuleProperty == null) {
                                if (capsulePropertyConfig.Required) {
                                    throw new System.IO.IOException($"Capsule Property with NameInSeeq {capsulePropertyConfig.NameInSeeq} " +
                                        $"is Required and was not found on capsule with Start {capsule.Start}, End {capsule.End}");
                                } else {
                                    continue;
                                }
                            } else {
                                capsulePropertyCount++;
                            }
                        }
                        if (ignoreUnspecifiedProperties == false &&
                            capsule.Properties.FindAll(x => !config.CapsuleProperties.Exists(y => y.NameInSeeq == x.Name)).Count > 0) {
                            throw new System.IO.IOException($"Found Properties not defined by the ConditionConfiguration CapsuleProperties " +
                                $"list for Condition {seeqConditionName} on capsule with Start {capsule.Start}, End {capsule.End}");
                        }

                        capsulesInput.Capsules.Add(capsule);
                    }

                    if (capsulesInput.Capsules.Count > 0) {
                        int apiTries = 3;
                        while (apiTries > 0) {
                            try {
                                conditionsApi.AddCapsules(seeqConditionId, capsulesInput);
                                break;
                            } catch {
                                if (apiTries > 0) {
                                    apiTries--;
                                } else {
                                    log.Error($"Tried {apiTries} times to AddCapsules to Condition with ID {seeqConditionId}; " +
                                        $"no attempt succeeded.");
                                    throw;
                                }
                            }
                        }
                    }
                    if (newLastCachedTimestamp != null) {
                        itemsApi.SetProperty(seeqConditionId, "LastCachedTimestamp",
                            new PropertyInputV1 { Value = newLastCachedTimestamp, UnitOfMeasure = "string" });
                    }
                    itemsApi.SetProperty(seeqConditionId, "DatastoreStatus",
                            new PropertyInputV1 { Value = datastoreStatus, UnitOfMeasure = "string" });
                }
            } catch (Exception ex) {
                log.Error($"Call to SendData for file {data.Filename} failed due to exception: {ex.Message}", ex);
                success = false;
            }
            return success;
        }

        /// <summary>
        /// Checks whether the passed object is a long, in which case it is a valid Seeq time as epoch nanoseconds, or
        /// a string, in which case it should be parseable as a DateTime and be of the format yyyy-MM-ddTHH:mm:ss followed
        /// by optional decimal and fractional seconds and either Z or an offset of the form +/-HH:mm.  If allowUncertain
        /// is true, the string may end in a question mark.
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        public static bool IsValidSeeqTime(object time, bool allowUncertain = false) {
            if (time is long) {
                return true;
            } else if (time is string) {
                DateTime result;
                string pattern = @"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.*(Z|[+-][0-9]{2}:[0-9]{2})";
                if (allowUncertain) {
                    pattern += @"\??";
                }
                Regex regex = new Regex(pattern);
                return DateTime.TryParse((string)time, out result) && regex.IsMatch((string)time);
            } else {
                return false;
            }
        }
    }
}