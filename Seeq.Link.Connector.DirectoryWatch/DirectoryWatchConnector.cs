using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using Seeq.Link.Connector.DirectoryWatch.Config;
using Seeq.Link.SDK;
using Seeq.Link.SDK.Interfaces;
using Seeq.Sdk.Model;

namespace Seeq.Link.Connector.DirectoryWatch {

    public class DirectoryWatchConnector : IConnectorV2 {
        private const string DefaultConnectorPluginDir = @"C:\ProgramData\Seeq\data\plugins\connectors\DirectoryWatch";

        private IConnectorServiceV2 connectorService;
        private DirectoryWatchConnectorConfigV1 connectorConfig;

        private List<DirectoryWatchConnectionConfigV1> connectionConfigs = new List<DirectoryWatchConnectionConfigV1>();

        public const string DatasourceClass = "DirectoryWatch";

        public string Name => "DirectoryWatch Connector";

        public void Initialize(IConnectorServiceV2 connectorService) {
            this.connectorService = connectorService;

            // if AppServer has not started up, this call will fail and the Agent will retry initializing this connector
            // at a later time with exponential back-off
            this.ensureDirectoryWatchDatasourceExists();

            this.connectorConfig = (DirectoryWatchConnectorConfigV1)this.connectorService.LoadConfig(new ConfigObject[] { new DirectoryWatchConnectorConfigV1() });

            if (this.connectorConfig.FileReaderFolders == null || this.connectorConfig.FileReaderFolders.Count == 0) {
                this.connectorConfig.FileReaderFolders = new List<string> { Path.Combine(DefaultConnectorPluginDir, "DataFileReaders") };
            }

            if (this.connectorConfig.ConfigurationFolders == null || this.connectorConfig.ConfigurationFolders.Count == 0) {
                this.connectorConfig.ConfigurationFolders = new List<string> { Path.Combine(DefaultConnectorPluginDir, "Configurations") };
                var datasourceConfiguration = new DirectoryWatchConnectionConfigV1 {
                    Name = "Sample Configuration",
                    Id = "Some Unique Identifier",
                    Description = "A sample configuration.",
                    Enabled = false,
                    Reader = "Some Reader classname",
                    FileDirectories = new List<string> { @"Relative\Directory\Name", @"C:\Absolute\Directory\Name" },
                    SignalConfigurations = new List<SignalConfigurationV1> {new SignalConfigurationV1() {
                            NameInFile = "FileHeaderName",
                            NameInSeeq = "SeeqSignalName",
                            Description = "Some signal from some watched directory.",
                            InterpolationType = "linear",
                            MaximumInterpolation = "1 day",
                            Uom = "Some Seeq-compatible UOM, such as m/s"
                        }}
                };
                this.connectionConfigs.Add(datasourceConfiguration);
                // TODO, JamesPD, 15Nov2017: the above references to the program data folder need to be made relative to the actual data folder,
                // and once this is done, the folders need to be created and the example configuration saved to the Configurations folder.
                this.SaveConfig();
            } else {
                this.connectionConfigs = readConfigFiles(this.connectorConfig.ConfigurationFolders);
                var repeatedConfigIDs = this.connectionConfigs.GroupBy(x => x.Id).Where(x => x.Count() > 1);

                if (repeatedConfigIDs.Any()) {
                    this.connectorService.Log.Error("Duplicate config IDs detected in DirectoryWatch configurations; this must be resolved before any connections are established.");
                } else {
                    var readerLoader = new ReaderLoader();
                    var configuration = this.connectorConfig.DebugMode ? "Debug" : "Release";
                    var dllFolderGlob = @"\**\bin\" + configuration + @"\*Reader.dll";
                    var searchPaths = this.connectorConfig.FileReaderFolders.Select(x => x + dllFolderGlob).ToArray();
                    readerLoader.Initialize(searchPaths);
                    var readerInstantiators = readerLoader.LoadDataFileReaderDLLs(this.connectorConfig.DebugMode);
                    foreach (var datasourceConfig in this.connectionConfigs) {
                        if (readerInstantiators.TryGetValue(datasourceConfig.Reader, out var readerInstantiator)) {
                            var connection = new DirectoryWatchConnection(this, datasourceConfig, readerInstantiator);
                            this.connectorService.AddConnection(connection);
                        } else {
                            this.connectorService.Log.Error($"Failed to find reader {datasourceConfig.Reader} for configuration {datasourceConfig.Name}, skipping.");
                        }
                    }
                }
            }
        }

        public void Destroy() {
        }

        public void SaveConfig() {
            this.connectorService.SaveConfig(this.connectorConfig);
        }

        /// <summary>
        /// Gets or creates the stored-in-Seeq DirectoryWatch datasource.  Returns the DatasourceOutputV1.
        /// </summary>
        private void ensureDirectoryWatchDatasourceExists() {
            var datasourcesApi = this.connectorService.AgentService.ApiProvider.CreateDatasourcesApi();
            var datasourceOutputList = datasourcesApi.GetDatasources(DatasourceClass, DatasourceClass, 0, 2, false);
            if (datasourceOutputList.Datasources.Count >= 2) {
                throw new ArgumentException("Cannot Initialize Reader due to duplicate DirectoryWatch datasources.");
            }

            if (datasourceOutputList.Datasources.Count == 0) {
                datasourcesApi.CreateDatasource(new DatasourceInputV1 {
                    Name = DatasourceClass,
                    DatasourceClass = DatasourceClass,
                    DatasourceId = DatasourceClass,
                    Description = "StoredInSeeq Datasource for all DirectoryWatch-created Items for 0.45+",
                    StoredInSeeq = true
                });
            }
        }

        private List<DirectoryWatchConnectionConfigV1> readConfigFiles(List<string> configFolders) {
            var configs = new List<DirectoryWatchConnectionConfigV1>();
            foreach (var configFolder in configFolders) {
                string[] fileList = null;
                try {
                    fileList = Directory.GetFiles(configFolder);
                } catch (Exception ex) {
                    this.connectorService.Log.Error($"Failed to get a list of files for specified configuration folder {configFolder}", ex);
                }
                if (fileList != null && fileList.Length > 0) {
                    foreach (var filename in fileList) {
                        string json = null;
                        try {
                            if (File.Exists(filename)) {
                                json = File.ReadAllText(filename);
                            }
                        } catch (Exception ex) {
                            if (filename == null) {
                                this.connectorService.Log.Error($"Could not resolve config file name \"{filename}\" to file path:\n{ex.Message}", ex);
                            } else {
                                throw new IOException($"Could not read json file \"{filename}\" due to exception:\n{ex.Message}", ex);
                            }
                        }

                        DirectoryWatchConnectionConfigV1 config = null;
                        try {
                            config = (DirectoryWatchConnectionConfigV1)JsonConvert.DeserializeObject(json, typeof(DirectoryWatchConnectionConfigV1));
                        } catch (Exception e) {
                            throw new IOException($"Could not deserialize json file \"{filename}\":\n{e}");
                        }
                        if (config != null) {
                            configs.Add(config);
                        }
                    }
                }
            }
            return configs;
        }
    }
}