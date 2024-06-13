using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using com.seeq.link.messages.agent;
using log4net;
using Seeq.Link.Connector.DirectoryWatch.Config;
using Seeq.Link.Connector.DirectoryWatch.Interfaces;
using Seeq.Link.Connector.DirectoryWatch.Utilities;
using Seeq.Link.SDK;
using Seeq.Link.SDK.Interfaces;
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

    public class DirectoryWatchConnection : BaseDatasourceConnection<DirectoryWatchConnector> {
        public const string DirectoryWatchDatasourceClass = "DirectoryWatch";

        public override ILog Log => log;

        private DirectoryWatchConnectionConfigV1 connectionConfig;

        private Dictionary<string, DataFileDirectoryMonitor> dataFileDirectoryMonitors;
        private Func<Dictionary<string, string>, object> readerInstantiator;
        private DataFileReader reader;
        private bool includeSubdirectories;
        private Regex fileNameFilter;
        private Regex subdirectoryFilter;

        private bool readerInitialized = false;

        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        public DirectoryWatchConnection(IAgentService agentService, DirectoryWatchConnector connector,
            DirectoryWatchConnectionConfigV1 connectionConfig, Func<Dictionary<string, string>, object> readerInstantiator) :
            base(agentService, connector, DirectoryWatchDatasourceClass, connectionConfig.Name, connectionConfig.Id, new IndexingSchedule(),
                new DatasourceService[] { DatasourceService.SIGNAL }, 1, 10) {
            this.connectionConfig = connectionConfig;
            //this.readerType = readerInstantiators.Select(x => x.Key).Where(x => x.ToString() == connectionConfig.Reader).First();
            this.readerInstantiator = readerInstantiator;
        }

        public override bool IsIndexingDatasourceConnection() {
            return true;
        }

        public override bool IsIndexingScheduleSupported() {
            return false;
        }

        public override bool IsPullDatasourceConnection() {
            return false;
        }

        public override void Initialize() {
            if (this.connectionConfig.Enabled == false) {
                log.Info($"{connectionConfig.Name} connection disabled in config file");
                return;
            }

            if (connectionConfig.SignalConfigurations != null && connectionConfig.ConditionConfigurations != null) {
                throw new ArgumentException($"DirectoryWatch Reader Configuration with ID {connectionConfig.Id} has both a " +
                    $"ConditionConfigurations list and a SignalConfigurations list.");
            }

            this.GetOrCreateDatasource(true);
            if (connectionConfig.ConditionConfigurations != null) {
                this.reader = (DataFileReader)readerInstantiator(connectionConfig.ReaderConfiguration);
                this.reader.ConditionConfigurations = connectionConfig.ConditionConfigurations;
            } else {
                this.reader = (DataFileReader)readerInstantiator(connectionConfig.ReaderConfiguration);
                this.reader.SignalConfigurations = connectionConfig.SignalConfigurations;
            }
            this.reader.Connection = this;
            this.includeSubdirectories = this.connectionConfig.IncludeSubdirectories;
            this.dataFileDirectoryMonitors = new Dictionary<string, DataFileDirectoryMonitor>();
            // Default regex matches anything that doesn't end in .imported or .importing
            this.fileNameFilter = string.IsNullOrWhiteSpace(this.connectionConfig.FileNameFilter) ?
                new Regex(@"^(?=^(?!.*\.imported$).*$)(?=^(?!.*\.importing$).*$).*$") :
                new Regex(this.connectionConfig.FileNameFilter);
            this.subdirectoryFilter = string.IsNullOrWhiteSpace(this.connectionConfig.SubdirectoryFilter) ?
                new Regex(".*") :
                new Regex(this.connectionConfig.SubdirectoryFilter);
            this.Enable();
        }

        public override void MetadataSync(SyncMode syncMode) {
            // Put a green check in the Connections list
            this.SetSyncStatus(SyncStatus.SYNC_COMPLETE);
        }

        protected override void Connect() {
            // confirm that all specified folders and files can be found
            // initialize File Watchers
            string connectingString = "Searching for folders specified in config files and setting up file watchers";

            this.SetState(ConnectionState.CONNECTING, connectingString);
            this.MetadataSync(SyncMode.Full);
            foreach (string directory in this.connectionConfig.FileDirectories) {
                string dir = directory.EndsWith("\\") ? directory : directory + "\\";
                if (System.IO.Directory.Exists(dir) == false) {
                    string failedToConnect = string.Format("Failed to connect to directory {0}", dir);
                    log.Error(failedToConnect);
                    this.SetState(ConnectionState.DISCONNECTED, failedToConnect);
                    return;
                }
            }
            this.SetState(ConnectionState.CONNECTED, "All directories found; initializing watches.");

            if (readerInitialized == false) {
                readerInitialized = this.reader.Initialize();
                // This pause is to give time to appserver to process the results of the reader initialization before ReadFile attempts to make use of them
                System.Threading.Thread.Sleep(3000);
            }
            if (readerInitialized) {
                foreach (string directory in this.connectionConfig.FileDirectories) {
                    string dir = directory.EndsWith("\\") ? directory : directory + "\\";
                    try {
                        if (System.IO.Directory.Exists(dir)) {
                            this.dataFileDirectoryMonitors[dir] =
                                new DataFileDirectoryMonitor(dir, this.fileNameFilter, this.subdirectoryFilter, this.includeSubdirectories, this.reader, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(1));
                            this.dataFileDirectoryMonitors[dir].Initialize();
                        } else {
                            string failedToConnect = string.Format("Failed to connect to directory {0}", dir);
                            log.Error(failedToConnect);
                            this.SetState(ConnectionState.DISCONNECTED, failedToConnect);
                            break;
                        }
                    } catch (Exception ex) {
                        string failedToConnect = string.Format("Failed to establish connection named {0} due to exception: {1}",
                            this.connectionConfig.Name, ex.Message);
                        log.Error(failedToConnect, ex);
                        this.SetState(ConnectionState.DISCONNECTED, failedToConnect);
                        break;
                    }
                }
            } else {
                this.SetState(ConnectionState.DISCONNECTED, "Failed to connect due to failed reader initialization");
                log.Error("Failed to connect due to failed reader initialization");
            }
        }

        protected override void Disconnect() {
            foreach (string monitor in this.dataFileDirectoryMonitors.Keys) {
                this.dataFileDirectoryMonitors[monitor].Stop();
            }
            log.Info("A disconnect request was completed.");
            this.SetState(ConnectionState.DISCONNECTED, "A disconnect request was completed.");
        }

        protected override void Monitor() {
            // confirm that all specified folders and files can be found
            // if so, set connection state to connected;
            // if not, set connection state to disconnected
            // if necessary, reinitiate file watchers

            ConnectionState newState = ConnectionState.DISCONNECTED; // default in case no FileDirectories specified
            string stateChangeMessage = null;
            foreach (string directory in this.connectionConfig.FileDirectories) {
                string dir = directory.EndsWith("\\") ? directory : directory + "\\";
                try {
                    if (System.IO.Directory.Exists(dir) && this.dataFileDirectoryMonitors.ContainsKey(dir)
                        && this.dataFileDirectoryMonitors[dir] != null) {
                        newState = ConnectionState.CONNECTED;
                    } else {
                        stateChangeMessage = string.Format("Connection became disconnected due to failed directory check for directory: {0}", directory);
                        newState = ConnectionState.DISCONNECTED;
                        break;
                    }
                } catch (Exception ex) {
                    newState = ConnectionState.DISCONNECTED;
                    stateChangeMessage = string.Format("Connection became disconnected due to exception during connection check: {0}", ex.Message);
                    break;
                }
            }
            if (this.State != newState) {
                if (newState == ConnectionState.CONNECTED) {
                    this.SetState(newState, "Connection restored");
                    log.Info($"Directory watch connection {this.connectionConfig.Name} restored for all connected directories.");
                } else {
                    this.SetState(newState, stateChangeMessage);
                    log.Error($"Directory watch connection monitor determined connection {this.connectionConfig.Name} to be disconnected." +
                        $"  The following reason was indicated: {stateChangeMessage}");
                }
            }
        }
    }
}