using System.Collections.Generic;
using Seeq.Link.Connector.DirectoryWatch.Config;
using Seeq.Sdk.Model;

namespace Seeq.Link.Connector.DirectoryWatch {

    public class DirectoryWatchData {
        public string Filename { get; set; }
        public DirectoryWatchConnection Connection { get; set; }
        public string PathSeparator { get; set; }
        public List<SignalConfigurationV1> SignalConfigurations { get; set; }
        public Dictionary<string, List<SampleInputV1>> SeeqSignalData { get; set; }
        public string ScopedTo { get; set; }
    }
}