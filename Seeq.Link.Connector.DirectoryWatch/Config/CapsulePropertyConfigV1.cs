using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Seeq.Link.Connector.DirectoryWatch.Config {

    public class CapsulePropertyConfigV1 {
        public string NameInSeeq { get; set; }
        public string NameInFile { get; set; }
        public string UnitOfMeasure { get; set; }
        public bool Required { get; set; }
    }
}