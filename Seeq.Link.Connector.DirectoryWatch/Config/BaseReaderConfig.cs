using System;
using System.Collections.Generic;

namespace Seeq.Link.Connector.DirectoryWatch.Config {

    public abstract class BaseReaderConfig {
        public bool DebugMode { get; set; }

        public int MaxFilesPerDirectory { get; set; }

        public int MaxFileSizeInKB { get; set; }

        protected BaseReaderConfig(Dictionary<string, string> readerConfiguration, bool debugMode) {
            this.DebugMode = debugMode;

            this.MaxFilesPerDirectory =
                this.getValueOrDefault(readerConfiguration, nameof(this.MaxFilesPerDirectory), 500);
            this.MaxFileSizeInKB =
                this.getValueOrDefault(readerConfiguration, nameof(this.MaxFileSizeInKB), 50);
        }

        protected T getValueOrDefault<T>(Dictionary<string, string> readerConfiguration, string key, T fallbackValue) {
            if (readerConfiguration.TryGetValue(key, out var readValue)) {
                return (T) Convert.ChangeType(readValue, typeof(T));
            }

            return fallbackValue;
        }
    }
}
