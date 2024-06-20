using System.Collections.Generic;
using Seeq.Link.Connector.DirectoryWatch.Config;
using Seeq.Link.SDK.Interfaces;

namespace Seeq.Link.Connector.DirectoryWatch.Interfaces {

    public abstract class DataFileReader : IDataFileReader {
        public IDatasourceConnectionServiceV2 ConnectionService { get; set; }

        public abstract bool Initialize();

        public abstract void ReadFile(string name);

        public virtual List<SignalConfigurationV1> SignalConfigurations { get; set; }

        public virtual List<ConditionConfigurationV1> ConditionConfigurations { get; set; }
    }
}