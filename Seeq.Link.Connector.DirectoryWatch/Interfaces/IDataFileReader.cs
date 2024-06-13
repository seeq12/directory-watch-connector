namespace Seeq.Link.Connector.DirectoryWatch.Interfaces {

    internal interface IDataFileReader {

        bool Initialize();

        void ReadFile(string name);
    }
}