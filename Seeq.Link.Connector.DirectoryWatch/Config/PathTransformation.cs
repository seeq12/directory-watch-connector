using System.Collections.Generic;

namespace Seeq.Link.Connector.DirectoryWatch.Config {

    public class PathTransformation {
        public Dictionary<string, string> FieldCaptures { get; set; }
        public string Output { get; set; }
    }
}