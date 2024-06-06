using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Seeq.Link.Connector.DirectoryWatch.Config {

    public class ConditionConfigurationV1 {
        public string ConditionName { get; set; }
        public string CapsuleStartField { get; set; }
        public string CapsuleEndField { get; set; }
        public string CapsuleDurationField { get; set; }
        public string DefaultDuration { get; set; }
        public string MaximumDuration { get; set; }
        public List<PathTransformation> PathTransformations { get; set; }
        public bool Required { get; set; }
        public List<CapsulePropertyConfigV1> CapsuleProperties { get; set; }
    }
}