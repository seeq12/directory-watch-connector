using System;
using System.Collections.Generic;
using System.Linq;
using Seeq.Link.Connector.DirectoryWatch.Config;

namespace Seeq.Link.Connector.DirectoryWatch.DataFileReaders {

    public class NarrowFileReaderConfigV1 : BaseReaderConfig {
        public int HeaderRow { get; set; } //This is the 1-based row, counting from the first row of the CSV file, that contains the headers.
        public int FirstDataRow { get; set; }
        public string TimestampHeaders { get; set; }
        public string TimestampFormat { get; set; }
        public string TimeZone { get; set; }
        public string SignalNameHeader { get; set; }
        public string SignalPrefixForDisambiguation { get; set; }
        public string ValueHeader { get; set; }
        public int RecordsPerDataPacket { get; set; }
        public string Delimiter { get; set; }
        public string CultureInfo { get; set; }
        public bool PostInvalidSamplesInsteadOfSkipping { get; set; }

        public NarrowFileReaderConfigV1(Dictionary<string, string> readerConfiguration, bool debugMode): base(readerConfiguration, debugMode) {
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
                this.SignalNameHeader = readerConfiguration["SignalNameHeader"];
                this.SignalPrefixForDisambiguation = readerConfiguration.ContainsKey("SignalPrefixForDisambiguation") ? readerConfiguration["SignalPrefixForDisambiguation"] : "";
                this.ValueHeader = readerConfiguration["ValueHeader"];
                this.RecordsPerDataPacket = Convert.ToInt32(readerConfiguration["RecordsPerDataPacket"]);
                if (readerConfiguration.ContainsKey("Delimiter")) {
                    this.Delimiter = readerConfiguration["Delimiter"];
                } else {
                    this.Delimiter = ",";
                }
                this.PostInvalidSamplesInsteadOfSkipping = readerConfiguration.ContainsKey("PostInvalidSamplesInsteadOfSkipping") ?
                    Convert.ToBoolean(readerConfiguration["PostInvalidSamplesInsteadOfSkipping"]) : false;
                this.CultureInfo = readerConfiguration.ContainsKey("CultureInfo") ? readerConfiguration["CultureInfo"] : null;
            } catch (KeyNotFoundException ex) {
                string readerConfigDict = string.Join(",\n", readerConfiguration.Select(x => x.Key + ": " + x.Value));
                string readerConfigDef = string.Join(",\n", this.GetType().GetProperties().ToList());
                string errMsg = "NarrowFileReader could not be initialized.  The expected fields for " +
                    $"the reader configuration are:\n" + readerConfigDef + "\nbut received:\n" + readerConfigDict;
                throw new ArgumentException(errMsg, ex);
            }
        }
    }
}