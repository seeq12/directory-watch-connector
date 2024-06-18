using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using Seeq.Link.Connector.DirectoryWatch.Config;
using Seeq.Link.SDK;
using Seeq.Link.SDK.Interfaces;
using Seeq.Sdk.Api;
using Seeq.Sdk.Model;

namespace Seeq.Link.Connector.DirectoryWatch {

    public class DirectoryWatchConnector : BaseConnector<DirectoryWatchConnectorConfigV1>, IConnector {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private List<DirectoryWatchConnectionConfigV1> connectionConfigs = new List<DirectoryWatchConnectionConfigV1>();

        public override string Name => "DirectoryWatch Connector";

        /// <summary>
        /// Gets or creates the stored-in-Seeq DirectoryWatch datasource.  Returns the DatasourceOutputV1.
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        private static DatasourceOutputV1 getOrCreateDirectoryWatchDatasource(IAgentService agentService) {
            IDatasourcesApi datasourcesApi = agentService.ApiProvider.CreateDatasourcesApi();
            string directoryWatch = "DirectoryWatch";
            DatasourceOutputListV1 datasourceOutputList = datasourcesApi.GetDatasources(directoryWatch, directoryWatch, 0, 2, false);
            if (datasourceOutputList.Datasources.Count >= 2) {
                throw new ArgumentException("Cannot Initialize Reader due to duplicate DirectoryWatch datasources.");
            } else if (datasourceOutputList.Datasources.Count == 0) {
                DatasourceOutputV1 datasourceOutput = datasourcesApi.CreateDatasource(new DatasourceInputV1 {
                    Name = directoryWatch,
                    DatasourceClass = directoryWatch,
                    DatasourceId = directoryWatch,
                    Description = "StoredInSeeq Datasource for all DirectoryWatch-created Items for 0.45+",
                    StoredInSeeq = true
                });
                return datasourceOutput;
            } else {
                return datasourceOutputList.Datasources[0];
            }
        }

        /// <summary>
        /// Eventually gets a datasource from appserver.  To avoid cluttering up the log, this method
        /// intentionally swallows most of the ApiExceptions while waiting for the license.  However,
        /// it retries frequently, since Agent.Initialize cannot proceed to the initialization of
        /// the other connectors until it returns.
        /// </summary>
        /// <returns>The DirectoryWatch datasource</returns>
        private DatasourceOutputV1 getDatasourceEventually(IAgentService agentService) {
            ISystemApi systemApi = agentService.ApiProvider.CreateSystemApi();

            int secondsBeforeRetry = 10;
            int secondsBeforeInfo = 60;
            int secondsSlept = 0;
            while (true) {
                try {
                    return getOrCreateDirectoryWatchDatasource(agentService);
                } catch {
                    if (secondsSlept >= secondsBeforeInfo) {
                        secondsSlept = 0;
                        secondsBeforeInfo = Math.Min(3600, 2 * secondsBeforeInfo);
                        log.Info("DirectoryWatch Connector is continuing to wait for appserver to be ready");
                    }
                    System.Threading.Thread.Sleep(secondsBeforeRetry * 1000);
                    secondsSlept += secondsBeforeRetry;
                }
            }
        }

        public override void Initialize(IAgentService agentService) {
            base.Initialize(agentService, new ConfigObject[] { new DirectoryWatchConnectorConfigV1() });

            getDatasourceEventually(agentService);

            if (this.Config.FileReaderFolders == null || this.Config.FileReaderFolders.Count == 0) {
                this.Config.FileReaderFolders = new List<string> { @"C:\ProgramData\Seeq\data\plugins\connectors\DirectoryWatch\DataFileReaders" };
            }

            if (this.Config.ConfigurationFolders == null || this.Config.ConfigurationFolders.Count == 0) {
                this.Config.ConfigurationFolders = new List<string> { @"C:\ProgramData\Seeq\data\plugins\connectors\DirectoryWatch\Configurations" };
                DirectoryWatchConnectionConfigV1 datasourceConfiguration = new DirectoryWatchConnectionConfigV1 {
                    Name = "Sample Configuration",
                    Id = "Some Unique Identifier",
                    Description = "A sample configuration.",
                    Enabled = false,
                    Reader = "Some Reader classname",
                    FileDirectories = new List<string> { @"Relative\Directory\Name", @"C:\Absolute\Directory\Name" },
                    SignalConfigurations = new List<SignalConfigurationV1> {
                            new SignalConfigurationV1() {
                                NameInFile = "FileHeaderName",
                                NameInSeeq = "SeeqSignalName",
                                Description = "Some signal from some watched directory.",
                                InterpolationType = "linear",
                                MaximumInterpolation = "1 day",
                                Uom = "Some Seeq-compatible UOM, such as m/s"
                            }
                        }
                };
                this.connectionConfigs.Add(datasourceConfiguration);
                // TODO, JamesPD, 15Nov2017: the above references to the program data folder need to be made relative to the actual data folder,
                // and once this is done, the folders need to be created and the example configuration saved to the Configurations folder.
                this.SaveConfig();
            } else {
                this.connectionConfigs = readConfigFiles(this.Config.ConfigurationFolders);
                var repeatedConfigIDs = this.connectionConfigs.GroupBy(x => x.Id).Where(x => x.Count() > 1);

                if (repeatedConfigIDs.Any()) {
                    log.Error("Duplicate config IDs detected in DirectoryWatch configurations; this must be resolved before any connections are established.");
                } else {
                    ReaderLoader readerLoader = new ReaderLoader();
                    string configuration = this.Config.DebugMode ? "Debug" : "Release";
                    string dllFolderGlob = @"\**\bin\" + configuration + @"\*Reader.dll";
                    string[] searchPaths = this.Config.FileReaderFolders.Select(x => x + dllFolderGlob).ToArray();
                    readerLoader.Initialize(searchPaths);
                    Dictionary<string, Func<Dictionary<string, string>, object>> readerInstantiators = readerLoader.LoadDataFileReaderDLLs(this.Config.DebugMode);
                    foreach (DirectoryWatchConnectionConfigV1 datasourceConfig in this.connectionConfigs) {
                        if (readerInstantiators.TryGetValue(datasourceConfig.Reader, out var readerInstantiator)) {
                            DirectoryWatchConnection connection = new DirectoryWatchConnection(this.AgentService, this,
                                datasourceConfig, readerInstantiator);
                            this.InitializeConnection(connection);
                        } else {
                            log.Error($"Failed to find reader {datasourceConfig.Reader} for configuration {datasourceConfig.Name}, skipping.");
                        }
                    }
                }
            }
        }

        private List<DirectoryWatchConnectionConfigV1> readConfigFiles(List<string> configFolders) {
            List<DirectoryWatchConnectionConfigV1> configs = new List<DirectoryWatchConnectionConfigV1>();
            foreach (string configFolder in configFolders) {
                string[] fileList = null;
                try {
                    fileList = Directory.GetFiles(configFolder);
                } catch (Exception ex) {
                    log.Error($"Failed to get a list of files for specified configuration folder {configFolder}", ex);
                }
                if (fileList != null && fileList.Length > 0) {
                    foreach (string filename in fileList) {
                        string json = null;
                        try {
                            if (File.Exists(filename)) {
                                json = File.ReadAllText(filename);
                            }
                        } catch (Exception ex) {
                            if (filename == null) {
                                log.Error($"Could not resolve config file name \"{filename}\" to file path:\n{ex.Message}", ex);
                            } else {
                                throw new IOException($"Could not read json file \"{filename}\" due to exception:\n{ex.Message}", ex);
                            }
                        }
                        DirectoryWatchConnectionConfigV1 config = null;
                        try {
                            config = (DirectoryWatchConnectionConfigV1)JsonConvert.DeserializeObject(json, typeof(DirectoryWatchConnectionConfigV1));
                        } catch (Exception e) {
                            throw new IOException(string.Format("Could not deserialize json file \"{0}\":\n{1}", filename, e));
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