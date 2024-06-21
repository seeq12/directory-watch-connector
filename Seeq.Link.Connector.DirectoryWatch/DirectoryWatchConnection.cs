using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Seeq.Link.Connector.DirectoryWatch.Config;
using Seeq.Link.Connector.DirectoryWatch.Interfaces;
using Seeq.Link.Connector.DirectoryWatch.Utilities;
using Seeq.Link.SDK;
using Seeq.Link.SDK.Interfaces;
using Seeq.Sdk.Model;
using ConnectionState = Seeq.Link.SDK.Interfaces.ConnectionState;

namespace Seeq.Link.Connector.DirectoryWatch {
    // Create a file watcher for the master config file, which specifies the connector config files.
    // The connector config files specify the watched directories for the connectors

    // Create a file watcher for each watched config file

    /**
     * The config files specify the paths for the data to watch; now watch these files.
     * To avoid having to track all changes to each file being watched, a 1-1 mapping is
     * established between the source signal and the Seeq signal.  The Seeq signal being updated
     * stores its own notion of "lastMetadataSync", called "lastRecordedSample", which is
     * the timestamp of the last recorded sample.  This timestamp is used by the FileWatcher
     * OnModify method to determine which data to send to the Seeq server when the file changes.
     * Since the DataId used by Seeq in CreateOrUpdate calls from the C# SDK is ignored in the
     * current release (R16.0.32.01), the Description is used to store a hierarchical name for the
     * signal; this name is of the form Company/Project/Experiment/TestCondition/Signal
     */

    public class DirectoryWatchConnection : IDatasourceConnectionV2 {
        private IDatasourceConnectionServiceV2 connectionService;
        private readonly DirectoryWatchConnectionConfigV1 connectionConfig;
        private readonly DirectoryWatchConnector connector;

        private Dictionary<string, DataFileDirectoryMonitor> dataFileDirectoryMonitors;
        private Func<Dictionary<string, string>, object> readerInstantiator;
        private DataFileReader reader;
        private bool includeSubdirectories;
        private Regex fileNameFilter;
        private Regex subdirectoryFilter;
        private bool readerInitialized = false;

        public string DatasourceClass => DirectoryWatchConnector.DatasourceClass;
        public string DatasourceName => this.connectionConfig.Name;
        public string DatasourceId => this.connectionConfig.Id;

        public DirectoryWatchConnection(DirectoryWatchConnector connector,
            DirectoryWatchConnectionConfigV1 connectionConfig,
            Func<Dictionary<string, string>, object> readerInstantiator) {
            this.connector = connector;
            this.connectionConfig = connectionConfig;
            this.readerInstantiator = readerInstantiator;
        }

        public void Initialize(IDatasourceConnectionServiceV2 connService) {
            this.connectionService = connService;

            if (this.connectionConfig.Enabled == false) {
                this.connectionService.Log.Info($"{connectionConfig.Name} connection disabled in config file");
                return;
            }

            if (connectionConfig.SignalConfigurations != null && connectionConfig.ConditionConfigurations != null) {
                throw new ArgumentException(
                    $"DirectoryWatch Reader Configuration with ID {connectionConfig.Id} has both a " +
                    "ConditionConfigurations list and a SignalConfigurations list.");
            }

            if (connectionConfig.ConditionConfigurations != null) {
                this.reader = (DataFileReader)readerInstantiator(connectionConfig.ReaderConfiguration);
                this.reader.ConditionConfigurations = connectionConfig.ConditionConfigurations;
            } else {
                this.reader = (DataFileReader)readerInstantiator(connectionConfig.ReaderConfiguration);
                this.reader.SignalConfigurations = connectionConfig.SignalConfigurations;
            }

            this.reader.ConnectionService = this.connectionService;
            this.includeSubdirectories = this.connectionConfig.IncludeSubdirectories;
            this.dataFileDirectoryMonitors = new Dictionary<string, DataFileDirectoryMonitor>();
            // Default regex matches anything that doesn't end in .imported or .importing
            this.fileNameFilter = string.IsNullOrWhiteSpace(this.connectionConfig.FileNameFilter)
                ? new Regex(@"^(?=^(?!.*\.imported$).*$)(?=^(?!.*\.importing$).*$).*$")
                : new Regex(this.connectionConfig.FileNameFilter);
            this.subdirectoryFilter = string.IsNullOrWhiteSpace(this.connectionConfig.SubdirectoryFilter)
                ? new Regex(".*")
                : new Regex(this.connectionConfig.SubdirectoryFilter);

            this.connectionService.Enable();
        }

        public void Destroy() {
            this.Disconnect();
        }

        public void Connect() {
            // confirm that all specified folders and files can be found
            // initialize File Watchers
            this.connectionService.Log.Info("Searching for folders specified in config files and setting up file watchers");
            this.connectionService.ConnectionState = ConnectionState.CONNECTING;

            foreach (var directory in this.connectionConfig.FileDirectories) {
                var dir = directory.EndsWith("\\") ? directory : directory + "\\";
                if (Directory.Exists(dir) == false) {
                    var failedToConnect = $"Failed to connect to directory {dir}";
                    this.connectionService.Log.Error(failedToConnect);
                    this.connectionService.ConnectionState = ConnectionState.DISCONNECTED;
                    return;
                }
            }

            this.connectionService.Log.Info("All directories found; initializing watches.");
            this.connectionService.ConnectionState = ConnectionState.CONNECTED;

            if (readerInitialized == false) {
                readerInitialized = this.reader.Initialize();
                // This pause is to give time to appserver to process the results of the reader initialization before ReadFile attempts to make use of them
                System.Threading.Thread.Sleep(3000);
            }

            if (readerInitialized) {
                foreach (var directory in this.connectionConfig.FileDirectories) {
                    var dir = directory.EndsWith("\\") ? directory : directory + "\\";
                    try {
                        if (Directory.Exists(dir)) {
                            this.dataFileDirectoryMonitors[dir] =
                                new DataFileDirectoryMonitor(dir, this.fileNameFilter, this.subdirectoryFilter,
                                    this.includeSubdirectories, this.reader, TimeSpan.FromSeconds(5),
                                    TimeSpan.FromSeconds(1));
                            this.dataFileDirectoryMonitors[dir].Initialize();
                        } else {
                            var failedToConnect = $"Failed to connect to directory {dir}";
                            this.connectionService.Log.Error(failedToConnect);
                            this.connectionService.ConnectionState = ConnectionState.DISCONNECTED;
                            break;
                        }
                    } catch (Exception ex) {
                        var failedToConnect =
                            $"Failed to establish connection named {this.connectionConfig.Name} due to exception: {ex.Message}";
                        this.connectionService.Log.Error(failedToConnect, ex);
                        this.connectionService.ConnectionState = ConnectionState.DISCONNECTED;
                        break;
                    }
                }
            } else {
                this.connectionService.Log.Error("Failed to connect due to failed reader initialization");
                this.connectionService.ConnectionState = ConnectionState.DISCONNECTED;
            }
        }

        public bool Monitor() {
            if (!this.connectionConfig.FileDirectories.Any()) {
                return false;
            }

            try {
                return this.connectionConfig.FileDirectories
                    .All(directory => Directory.Exists(directory)
                                      && this.dataFileDirectoryMonitors.ContainsKey(directory)
                                      && this.dataFileDirectoryMonitors[directory] != null);
            } catch (Exception ex) {
                this.connectionService.Log.Error("An error occurred during connection check", ex);
                return false;
            }
        }

        public void Disconnect() {
            foreach (var monitor in this.dataFileDirectoryMonitors.Keys) {
                this.dataFileDirectoryMonitors[monitor].Stop();
            }

            this.connectionService.Log.Info("A disconnect request was completed.");
            this.connectionService.ConnectionState = ConnectionState.DISCONNECTED;
        }

        public void SaveConfig() {
            this.connector.SaveConfig();
        }
    }
}