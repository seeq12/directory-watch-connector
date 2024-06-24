using System;
using System.Collections.Generic;
using System.IO;
using log4net;
using Seeq.Link.Connector.DirectoryWatch.Config;
using Seeq.Link.SDK.Interfaces;

namespace Seeq.Link.Connector.DirectoryWatch.Interfaces {

    public abstract class DataFileReader : IDataFileReader {
        public IDatasourceConnectionServiceV2 ConnectionService { get; set; }
        private static readonly ILog Log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod()?.DeclaringType);
        
        public BaseReaderConfig ReaderConfig { get; protected set; }
        
        public DirectoryWatchConnection Connection { get; set; }

        public virtual List<SignalConfigurationV1> SignalConfigurations { get; set; }

        public virtual List<ConditionConfigurationV1> ConditionConfigurations { get; set; }

        public abstract bool Initialize();

        public abstract void ReadFile(string name);

        public void ValidateAndReadFile(string originalFileName, string modifiedFileName) {
            this.validateFileSizeLimit(originalFileName, modifiedFileName);
            this.ReadFile(modifiedFileName);
        }

        private void validateFileSizeLimit(string originalFileName, string modifiedFileName) {
            var fileSize = new FileInfo(modifiedFileName).Length;
            var fileSizeLimitInBytes = this.ReaderConfig.MaxFileSizeInKB * 1024L;

            if (fileSize > fileSizeLimitInBytes) {
                Log.ErrorFormat("File with name: '{0}', has size: {1}KB which exceeds the configured file size limit: {2}KB", originalFileName, Math.Round(fileSize / 1024m), this.ReaderConfig.MaxFileSizeInKB);
                throw new InvalidOperationException("The file to be read exceeds the configured file size limits");
            }
        }
    }
}