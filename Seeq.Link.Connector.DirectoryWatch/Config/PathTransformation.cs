using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Seeq.Link.Connector.DirectoryWatch.Config {

    public class PathTransformation {
        public Dictionary<string, string> FieldCaptures { get; set; }
        public string Output { get; set; }
    }
}