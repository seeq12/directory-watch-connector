using System;
using System.Collections.Generic;
using System.Linq;

namespace Seeq.Link.Connector.DirectoryWatch.DataFileReaders {

    public class TimestampAssetTagsCsvReaderConfigV1 {
        public int HeaderRow { get; set; } //This is the 1-based row, counting from the first row of the CSV file, that contains the headers.
        public int FirstDataRow { get; set; }
        public string TimestampHeaders { get; set; }
        public string TimestampFormat { get; set; }
        public string TimeZone { get; set; }
        public bool EnforceTimestampOrder { get; set; }
        public string AssetTreeRootName { get; set; }
        public string AssetPathSeparator { get; set; }
        public string AssetPathHeaders { get; set; }
        public int RecordsPerDataPacket { get; set; }
        public bool DebugMode { get; set; }
        public string Delimiter { get; set; }
        public string CultureInfo { get; set; }
        public string ScopedTo { get; set; }
        public bool PostInvalidSamplesInsteadOfSkipping { get; set; }

        public TimestampAssetTagsCsvReaderConfigV1(Dictionary<string, string> readerConfiguration, bool debugMode) {
            try {
                this.DebugMode = debugMode;
                this.HeaderRow = Convert.ToInt32(readerConfiguration["HeaderRow"]);
                this.FirstDataRow = Convert.ToInt32(readerConfiguration["FirstDataRow"]);
                if (this.HeaderRow >= this.FirstDataRow) {
                    throw new ArgumentException("FirstDataRow must be greater than HeaderRow.");
                }
                this.TimestampHeaders = readerConfiguration["TimestampHeaders"];
                this.TimestampFormat = readerConfiguration["TimestampFormat"];
                this.TimeZone = readerConfiguration["TimeZone"];
                bool test;
                if (bool.TryParse(readerConfiguration["EnforceTimestampOrder"], out test)) {
                    this.EnforceTimestampOrder = test;
                } else {
                    this.EnforceTimestampOrder = true;
                }
                this.AssetTreeRootName = readerConfiguration["AssetTreeRootName"];
                this.AssetPathSeparator = readerConfiguration["AssetPathSeparator"];
                this.AssetPathHeaders = readerConfiguration["AssetPathHeaders"];
                this.RecordsPerDataPacket = Convert.ToInt32(readerConfiguration["RecordsPerDataPacket"]);
                if (readerConfiguration.TryGetValue("Delimiter", out var value)) {
                    this.Delimiter = value;
                } else {
                    this.Delimiter = ",";
                }
                this.PostInvalidSamplesInsteadOfSkipping = readerConfiguration.ContainsKey("PostInvalidSamplesInsteadOfSkipping") ?
                    Convert.ToBoolean(readerConfiguration["PostInvalidSamplesInsteadOfSkipping"]) : false;
                this.CultureInfo = readerConfiguration.ContainsKey("CultureInfo") ? readerConfiguration["CultureInfo"] : null;
                this.ScopedTo = readerConfiguration.ContainsKey("ScopedTo") ? readerConfiguration["ScopedTo"] : null;
            } catch (KeyNotFoundException ex) {
                string readerConfigDict = string.Join(",\n", readerConfiguration.Select(x => x.Key + ": " + x.Value));
                string readerConfigDef = string.Join(",\n", this.GetType().GetProperties().ToList());
                string errMsg = "TimestampAssetTagsCsvReader could not be initialized.  The expected fields for " +
                    $"the reader configuration are:\n" + readerConfigDef + "\nbut received:\n" + readerConfigDict;
                throw new ArgumentException(errMsg, ex);
            }
        }
    }
}