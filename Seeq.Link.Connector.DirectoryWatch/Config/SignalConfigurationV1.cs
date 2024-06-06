using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Seeq.Link.Connector.DirectoryWatch.Config {

    public class SignalConfigurationV1 {
        public string NameInSeeq { get; set; }
        public string NameInFile { get; set; }
        public string Description { get; set; }
        public string Uom { get; set; }
        public string InterpolationType { get; set; }
        public string MaximumInterpolation { get; set; }
        public bool Required { get; set; }
    }
}