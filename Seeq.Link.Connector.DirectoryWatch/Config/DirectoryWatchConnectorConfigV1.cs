using System.Collections.Generic;
using Seeq.Link.SDK;

namespace Seeq.Link.Connector.DirectoryWatch.Config {

    public class DirectoryWatchConnectorConfigV1 : ConfigObject {
        public bool DebugMode { get; set; } = false;

        public List<string> FileReaderFolders { get; set; }

        public List<string> ConfigurationFolders { get; set; }
    }
}