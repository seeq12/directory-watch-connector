using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Seeq.Sdk.Model;
using Seeq.Link.Connector.DirectoryWatch.Config;

namespace Seeq.Link.Connector.DirectoryWatch {

    /// <summary>
    /// This object is used to package up the data that is sent to Seeq via the DirectoryWatchUtilities method
    /// SendConditionData.  The Filename is for logging purposes.  The Connection should be the Reader's Connection
    /// property.  The AssetTreeRoot is an AssetOutputV1, but only the Name and DataID properties need to be set.  The
    /// PathSeparator is the string that will be used to split the SeeqConditionData keys to define the Asset tree paths
    /// for where to put the Conditions relative to the AssetTreeRoot - it is included to give flexibility in case the
    /// names of the Assets or Conditions should have certain characters in them on which we wouldn't want to split
    /// the path names unintentionally.  The ConditionConfigurations is a list of ConditionConfigurationV1 objects -
    /// see the docs for that object for more details.  The SeeqConditionData is a dictionary of capsule data that
    /// is to be posted to the Conditions defined by the ConditionConfigurations and located according to the keys
    /// of the dictionary, which are the paths through the Asset tree to the Conditions.  It should always be the
    /// case that any key in the SeeqConditionData end in a unique Condition name that is also a NameInSeeq of
    /// a ConditionConfigurationV1 in the ConditionConfigurations list.
    /// </summary>
    public class DirectoryWatchConditionData {
        public string Filename { get; set; }
        public DirectoryWatchConnection Connection { get; set; }
        public AssetOutputV1 AssetTreeRoot { get; set; }
        public string PathSeparator { get; set; }
        public List<ConditionConfigurationV1> ConditionConfigurations { get; set; }
        public Dictionary<string, List<CapsuleInputV1>> SeeqConditionData { get; set; }
    }
}