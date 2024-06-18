using System;
using System.Collections.Generic;
using System.Linq;
using Seeq.Link.Connector.DirectoryWatch.Config;

namespace Seeq.Link.Connector.DirectoryWatch.DataFileReaders {

    public class ConditionsWithPropertiesReaderConfigV1 : BaseReaderConfig {
        public int HeaderRow { get; set; } //This is the 1-based row, counting from the first row of the CSV file, that contains the headers.
        public int FirstDataRow { get; set; }
        public string TimestampFormat { get; set; }
        public string TimeZone { get; set; }
        public bool UseFilePathForHierarchy { get; set; }
        public string FilePathHierarchyRoot { get; set; }
        public bool FilePathHierarchyIncludesFilename { get; set; }
        public int RecordsPerDataPacket { get; set; }
        public string Delimiter { get; set; }

        public ConditionsWithPropertiesReaderConfigV1(Dictionary<string, string> readerConfiguration, bool debugMode): base(readerConfiguration, debugMode) {
            try {
                this.DebugMode = debugMode;
                this.HeaderRow = Convert.ToInt32(readerConfiguration["HeaderRow"]);
                this.FirstDataRow = Convert.ToInt32(readerConfiguration["FirstDataRow"]);
                if (this.HeaderRow >= this.FirstDataRow) {
                    throw new ArgumentException("FirstDataRow must be greater than HeaderRow.");
                }
                this.TimestampFormat = readerConfiguration["TimestampFormat"];
                this.TimeZone = readerConfiguration["TimeZone"];
                this.UseFilePathForHierarchy = Convert.ToBoolean(readerConfiguration["UseFilePathForHierarchy"]);
                this.FilePathHierarchyRoot = readerConfiguration["FilePathHierarchyRoot"];
                this.FilePathHierarchyIncludesFilename = Convert.ToBoolean(readerConfiguration["FilePathHierarchyIncludesFilename"]);
                this.RecordsPerDataPacket = Convert.ToInt32(readerConfiguration["RecordsPerDataPacket"]);
                if (readerConfiguration.ContainsKey("Delimiter")) {
                    this.Delimiter = readerConfiguration["Delimiter"];
                } else {
                    this.Delimiter = ",";
                }
            } catch (KeyNotFoundException ex) {
                string readerConfigDict = string.Join(",\n", readerConfiguration.Select(x => x.Key + ": " + x.Value));
                string readerConfigDef = string.Join(",\n", this.GetType().GetProperties().ToList());
                string errMsg = "ConditionsWithPropertiesReader could not be initialized.  The expected fields for " +
                    $"the reader configuration are:\n" + readerConfigDef + "\nbut received:\n" + readerConfigDict;
                throw new ArgumentException(errMsg, ex);
            }
        }
    }
}