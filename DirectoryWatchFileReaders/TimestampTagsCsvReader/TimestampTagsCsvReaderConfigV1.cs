using System;
using System.Collections.Generic;
using System.Linq;
using Seeq.Link.Connector.DirectoryWatch.Config;

namespace Seeq.Link.Connector.DirectoryWatch.DataFileReaders {

    public class TimestampTagsCsvReaderConfigV1 : BaseReaderConfig {
        public int HeaderRow { get; set; } //This is the 1-based row, counting from the first row of the CSV file, that contains the headers.
        public int FirstDataRow { get; set; }
        public string TimestampHeaders { get; set; }
        public string TimestampFormat { get; set; }
        public string TimeZone { get; set; }
        public bool EnforceTimestampOrder { get; set; }
        public bool UseFilePathForHierarchy { get; set; }
        public string FilePathHierarchyRoot { get; set; }
        public bool FilePathHierarchyIncludesFilename { get; set; }
        public int RecordsPerDataPacket { get; set; }
        public string Delimiter { get; set; }
        public string CultureInfo { get; set; }
        public string ScopedTo { get; set; }
        public bool PostInvalidSamplesInsteadOfSkipping { get; set; }

        public TimestampTagsCsvReaderConfigV1(Dictionary<string, string> readerConfiguration, bool debugMode): base(readerConfiguration, debugMode) {
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
                this.UseFilePathForHierarchy = Convert.ToBoolean(readerConfiguration["UseFilePathForHierarchy"]);
                this.FilePathHierarchyRoot = readerConfiguration["FilePathHierarchyRoot"];
                this.FilePathHierarchyIncludesFilename = Convert.ToBoolean(readerConfiguration["FilePathHierarchyIncludesFilename"]);
                this.RecordsPerDataPacket = Convert.ToInt32(readerConfiguration["RecordsPerDataPacket"]);

                this.Delimiter = this.getValueOrDefault(readerConfiguration, nameof(this.Delimiter), ",");
                this.PostInvalidSamplesInsteadOfSkipping = this.getValueOrDefault(readerConfiguration, nameof(this.PostInvalidSamplesInsteadOfSkipping), false);
                this.CultureInfo = this.getValueOrDefault<string>(readerConfiguration, nameof(this.CultureInfo), null);
                this.ScopedTo = this.getValueOrDefault<string>(readerConfiguration, nameof(this.ScopedTo), null);
            } catch (KeyNotFoundException ex) {
                string readerConfigDict = string.Join(",\n", readerConfiguration.Select(x => x.Key + ": " + x.Value));
                string readerConfigDef = string.Join(",\n", this.GetType().GetProperties().ToList());
                string errMsg = "TimestampTagsCsvReader could not be initialized.  The expected fields for " +
                    $"the reader configuration are:\n" + readerConfigDef + "\nbut received:\n" + readerConfigDict;
                throw new ArgumentException(errMsg, ex);
            }
        }
    }
}