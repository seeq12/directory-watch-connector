using System;
using System.Collections.Generic;
using System.Linq;
using Seeq.Link.Connector.DirectoryWatch.Config;

namespace OffsetTagsReader {

    internal class OffsetTagsReaderConfig : BaseReaderConfig {
        public string AssetTreeRootName { get; private set; }

        public string TimeZone { get; private set; }

        public int HeaderRow { get; private set; }
        public int FirstDataRow { get; private set; }

        public string TimeSpanHeader { get; private set; }
        public long TimeSpanFixed { get; private set; }
        public string TimeSpanUnits { get; private set; }

        public string FilePathHierarchyRoot { get; private set; }

        public bool UseFilePathForHierarchy { get; private set; }
        public bool UseFilenameRegexForLeafAsset { get; private set; }

        public string FixedBaseTime { get; private set; }
        public string FilenameDateCaptureRegex { get; private set; }
        public string FilenameDateFormatString { get; private set; }
        public string FilenameAssetCaptureRegex { get; private set; }

        public bool EnforceTimestampOrder { get; private set; }
        public bool SkipBadSamples { get; private set; }
        public bool SkipNullValueSamples { get; private set; }
        public bool PostInvalidSamplesInsteadOfSkipping { get; private set; }

        public int RecordsPerDataPacket { get; private set; }
        public string ScopedTo { get; set; }

        public OffsetTagsReaderConfig(Dictionary<string, string> readerConfig): base(readerConfig, false) {
            try {
                this.AssetTreeRootName = readerConfig["AssetTreeRootName"];
                if (string.IsNullOrWhiteSpace(this.AssetTreeRootName)) {
                    throw new ArgumentException("AssetTreeRootName cannot be blank");
                }

                this.TimeZone = readerConfig["TimeZone"];

                this.HeaderRow = Convert.ToInt32(readerConfig["HeaderRow"]);
                this.FirstDataRow = Convert.ToInt32(readerConfig["FirstDataRow"]);
                if (this.HeaderRow >= this.FirstDataRow) {
                    throw new ArgumentException("FirstDataRow must be greater than HeaderRow.");
                }

                if (string.IsNullOrWhiteSpace(readerConfig["TimeSpanHeader"]) ==
                    string.IsNullOrWhiteSpace(readerConfig["TimeSpanFixed"])) {
                    throw new ArgumentException("Exactly one of TimeSpanHeader and TimeSpanFixed must be null");
                }
                this.TimeSpanHeader = readerConfig["TimeSpanHeader"];
                this.TimeSpanFixed = Convert.ToInt64(readerConfig["TimeSpanFixed"]);

                this.TimeSpanUnits = readerConfig["TimeSpanUnits"];
                if (string.IsNullOrWhiteSpace(this.TimeSpanUnits)) {
                    throw new ArgumentException("TimeSpanUnits cannot be blank");
                }
                if ((new string[] { "Days", "Hours", "Minutes", "Seconds", "Milliseconds", "Microseconds", "Nanoseconds" })
                    .Contains(this.TimeSpanUnits) == false) {
                    throw new ArgumentException("TimeSpanUnits must be one of Days, Hours, Minutes, Seconds, Milliseconds, Microseconds, or Nanoseconds (case sensitive).");
                }

                this.FilePathHierarchyRoot = readerConfig["FilePathHierarchyRoot"];
                if (string.IsNullOrWhiteSpace(this.FilePathHierarchyRoot)) {
                    throw new ArgumentException("FilePathHierarchyRoot cannot be blank");
                }

                this.FixedBaseTime = readerConfig.ContainsKey("FixedBaseTime") ? readerConfig["FixedBaseTime"] : null;
                this.FilenameDateCaptureRegex = readerConfig.ContainsKey("FilenameDateCaptureRegex") ?
                    readerConfig["FilenameDateCaptureRegex"] : null;
                this.FilenameDateFormatString = readerConfig.ContainsKey("FilenameDateFormatString") ?
                    readerConfig["FilenameDateFormatString"] : null;
                if (string.IsNullOrWhiteSpace(this.FixedBaseTime)) {
                    if (string.IsNullOrWhiteSpace(this.FilenameDateCaptureRegex) ||
                        string.IsNullOrWhiteSpace(this.FilenameDateFormatString)) {
                        throw new ArgumentException("Configuration file must contain both a FilenameDateCaptureRegex and " +
                            "a FilenameDateFormatString if no FixedBaseTime is provided.");
                    }
                } else {
                    if (!string.IsNullOrWhiteSpace(this.FilenameDateCaptureRegex) ||
                        !string.IsNullOrWhiteSpace(this.FilenameDateFormatString)) {
                        throw new ArgumentException("Configuration file cannot include specifications for " +
                            "FilenameDateCaptureRegex or FilenameDateFormatString if FixedBaseTime is also specified");
                    }

                    if (!DateTime.TryParseExact(this.FixedBaseTime, "yyyy-MM-ddTHH:mm:ssK", null, System.Globalization.DateTimeStyles.None, out _) &&
                        !DateTime.TryParseExact(this.FixedBaseTime, "yyyy-MM-ddTHH:mm:ss.fffK", null, System.Globalization.DateTimeStyles.None, out _) &&
                        !DateTime.TryParseExact(this.FixedBaseTime, "yyyy-MM-ddTHH:mm:ss.fffffffK", null, System.Globalization.DateTimeStyles.None, out _)) {
                        throw new ArgumentException("FixedBaseTime must be in ISO8601 format and have 0, 3, or 7 decimal digits, e.g., 2018-11-02T16:02:43.1234567.");
                    }
                }

                this.FilenameAssetCaptureRegex = readerConfig["FilenameAssetCaptureRegex"];

                this.UseFilePathForHierarchy = Convert.ToBoolean(readerConfig["UseFilePathForHierarchy"]);
                this.UseFilenameRegexForLeafAsset = Convert.ToBoolean(readerConfig["UseFilenameRegexForLeafAsset"]);

                this.EnforceTimestampOrder = Convert.ToBoolean(readerConfig["EnforceTimestampOrder"]);
                this.SkipBadSamples = Convert.ToBoolean(readerConfig["SkipBadSamples"]);
                this.SkipNullValueSamples = Convert.ToBoolean(readerConfig["SkipNullValueSamples"]);

                this.RecordsPerDataPacket = readerConfig.ContainsKey("RecordsPerDataPacket") ?
                    Convert.ToInt32(readerConfig["RecordsPerDataPacket"]) : 1000;
                this.PostInvalidSamplesInsteadOfSkipping = readerConfig.ContainsKey("PostInvalidSamplesInsteadOfSkipping") ?
                    Convert.ToBoolean(readerConfig["PostInvalidSamplesInsteadOfSkipping"]) : false;
                this.ScopedTo = readerConfig.ContainsKey("ScopedTo") ? readerConfig["ScopedTo"] : null;
            } catch (KeyNotFoundException) {
                string configStr = string.Join(",\n  ", this.GetType().GetProperties().ToList());
                string readerConfigString = string.Join(",\n  ", readerConfig.Select(x => x.Key + ": " + x.Value));
                string msg = $"OffsetTagsReader configuration requires the following fields to be set:\n  {configStr}" +
                    $"\nHowever, only the following inputs were received:\n  " + readerConfigString;
                throw new ArgumentException(msg);
            }
        }
    }
}