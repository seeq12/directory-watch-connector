using System;
using System.Collections.Generic;
using Seeq.Link.SDK.Utilities;

namespace Seeq.Link.Connector.DirectoryWatch.Config {

    public abstract class BaseReaderConfig {
        public bool DebugMode { get; set; }

        public int MaxFilesPerDirectory { get; set; }

        public int MaxFileSizeInKB { get; set; }

        protected BaseReaderConfig(Dictionary<string, string> readerConfiguration, bool debugMode) {
            this.DebugMode = debugMode;

            var maxFilesPerDirectory =
                this.getValueOrDefault(readerConfiguration, nameof(this.MaxFilesPerDirectory), 500);
            var maxFileSizeInKB =
                this.getValueOrDefault(readerConfiguration, nameof(this.MaxFileSizeInKB), 5120); // 5MB

            Preconditions.CheckArgument(maxFilesPerDirectory > 0, "The maximum file count has to be a positive integer");
            Preconditions.CheckArgument(maxFileSizeInKB > 0, "The maximum file size has to be a positive integer");

            this.MaxFilesPerDirectory = maxFilesPerDirectory;
            this.MaxFileSizeInKB = maxFileSizeInKB;
        }

        protected T getValueOrDefault<T>(Dictionary<string, string> readerConfiguration, string key, T fallbackValue) {
            if (readerConfiguration.TryGetValue(key, out var readValue)) {
                return (T) Convert.ChangeType(readValue, typeof(T));
            }

            return fallbackValue;
        }
    }
}
