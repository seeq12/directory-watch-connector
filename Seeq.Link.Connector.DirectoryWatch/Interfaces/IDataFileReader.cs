using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Seeq.Link.Connector.DirectoryWatch.Interfaces {

    internal interface IDataFileReader {

        bool Initialize();

        void ReadFile(string name);
    }
}