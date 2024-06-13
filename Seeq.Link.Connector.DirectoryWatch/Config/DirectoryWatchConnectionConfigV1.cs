using System.Collections.Generic;
using Seeq.Link.SDK;

namespace Seeq.Link.Connector.DirectoryWatch.Config {

    public class DirectoryWatchConnectionConfigV1 : DatasourceConnectionConfig {
        public string Description { get; set; }
        public List<string> FileDirectories { get; set; }
        public bool IncludeSubdirectories { get; set; }
        public string FileNameFilter { get; set; }
        public string SubdirectoryFilter { get; set; }
        public string Reader { get; set; }
        public string SyncToken { get; set; }
        public Dictionary<string, string> ReaderConfiguration { get; set; }
        public List<SignalConfigurationV1> SignalConfigurations { get; set; }
        public List<ConditionConfigurationV1> ConditionConfigurations { get; set; }
    }
}