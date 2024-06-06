using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Seeq.Link.SDK;

namespace Seeq.Link.Connector.DirectoryWatch.Config {

    public class DirectoryWatchConnectorConfigV1 : ConfigObject {
        public bool DebugMode;
        public List<string> FileReaderFolders;
        public List<string> ConfigurationFolders;

        public List<DirectoryWatchConnectionConfigV1> DatasourceConfigurations;

        public DirectoryWatchConnectorConfigV1() {
            this.DebugMode = false;
            this.DatasourceConfigurations = new List<DirectoryWatchConnectionConfigV1>();
        }
    }
}