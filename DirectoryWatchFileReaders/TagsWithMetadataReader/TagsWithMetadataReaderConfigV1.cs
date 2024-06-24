using System;
using System.Collections.Generic;
using System.Linq;
using Seeq.Link.Connector.DirectoryWatch.Config;

namespace Seeq.Link.Connector.DirectoryWatch.DataFileReaders {

    public class TagsWithMetadataReaderConfigV1 : BaseReaderConfig {

        public enum MetadataRow { UOM, InterpolationType, MaximumInterpolation, Description, Headers }

        private int headerRow; //This is the 1-based row, counting from the first row of the CSV file, that contains the headers.

        public Dictionary<long, MetadataRow> MetadataRows { get; }

        public Dictionary<MetadataRow, string> MetadataDefaults { get; }

        public int FirstDataRow { get; }

        public string TimestampFormat { get; }

        public string Zone { get; }

        public bool EnforceTimestampOrder { get; }

        public bool UseFilePathForHierarchy { get; }

        public string FilePathHierarchyRoot { get; }

        public bool FilePathHierarchyIncludesFilename { get; }

        public string PathSeparator => @"\";

        public bool PostInvalidSamplesInsteadOfSkipping { get; }

        public int RecordsPerDataPacket { get; }

        public string ScopedTo { get; }

        public string FilenameRegexScopeCapture { get; }

        public string CultureInfoString { get; }

        public string Delimiter { get; }

        public TagsWithMetadataReaderConfigV1(Dictionary<string, string> readerConfiguration, bool debugMode) : base(readerConfiguration, debugMode) {
            try {
                this.MetadataRows = new Dictionary<long, MetadataRow>() {
                    { Convert.ToInt32(readerConfiguration["NameRow"]), MetadataRow.Headers }
                };
                if (readerConfiguration.ContainsKey("UOMRow")) {
                    this.MetadataRows.Add(Convert.ToInt32(readerConfiguration["UOMRow"]), MetadataRow.UOM);
                }
                if (readerConfiguration.ContainsKey("InterpStyleRow")) {
                    this.MetadataRows.Add(Convert.ToInt32(readerConfiguration["InterpStyleRow"]), MetadataRow.InterpolationType);
                }
                if (readerConfiguration.ContainsKey("MaxInterpRow")) {
                    this.MetadataRows.Add(Convert.ToInt32(readerConfiguration["MaxInterpRow"]), MetadataRow.MaximumInterpolation);
                }
                if (readerConfiguration.ContainsKey("DescriptionRow")) {
                    this.MetadataRows.Add(Convert.ToInt32(readerConfiguration["DescriptionRow"]), MetadataRow.Description);
                }

                this.MetadataDefaults = new Dictionary<MetadataRow, string>();
                this.MetadataDefaults.Add(MetadataRow.MaximumInterpolation,
                    readerConfiguration.ContainsKey("DefaultMaxInterp") ? readerConfiguration["DefaultMaxInterp"] :
                    null);
                this.MetadataDefaults.Add(MetadataRow.InterpolationType,
                    readerConfiguration.ContainsKey("DefaultInterpType") ? readerConfiguration["DefaultInterpType"] :
                    null);
                this.MetadataDefaults.Add(MetadataRow.UOM,
                    readerConfiguration.ContainsKey("DefaultUOM") ? readerConfiguration["DefaultUOM"] :
                    null);
                this.MetadataDefaults.Add(MetadataRow.Description,
                    readerConfiguration.ContainsKey("DefaultDescription") ? readerConfiguration["DefaultDescription"] :
                    null);

                this.headerRow = Convert.ToInt32(readerConfiguration["NameRow"]);
                this.FirstDataRow = Convert.ToInt32(readerConfiguration["FirstDataRow"]); // Moved to top for clarity, since it's required
                this.RecordsPerDataPacket = Convert.ToInt32(readerConfiguration["RecordsPerDataPacket"]);
                if (this.headerRow >= this.FirstDataRow) {
                    throw new ArgumentException("FirstDataRow must be greater than NameRow.");
                }
                this.TimestampFormat = readerConfiguration["TimestampFormat"];
                this.Zone = readerConfiguration["TimeZone"];
                if (bool.TryParse(readerConfiguration["EnforceTimestampOrder"], out _) == false) {
                    this.EnforceTimestampOrder = true;
                };
                this.UseFilePathForHierarchy = Convert.ToBoolean(readerConfiguration["UseFilePathForHierarchy"]);
                this.FilePathHierarchyRoot = readerConfiguration["FilePathHierarchyRoot"];
                this.FilePathHierarchyIncludesFilename = Convert.ToBoolean(readerConfiguration["FilePathHierarchyIncludesFilename"]);
                this.PostInvalidSamplesInsteadOfSkipping = readerConfiguration.ContainsKey("PostInvalidSamplesInsteadOfSkipping") ?
                    Convert.ToBoolean(readerConfiguration["PostInvalidSamplesInsteadOfSkipping"]) : false;
                this.ScopedTo = readerConfiguration.ContainsKey("ScopedTo") ? readerConfiguration["ScopedTo"] : null;
                this.FilenameRegexScopeCapture = readerConfiguration.ContainsKey("FilenameRegexScopeCapture") ?
                    readerConfiguration["FilenameRegexScopeCapture"] : null;
                if (this.ScopedTo != null && this.FilenameRegexScopeCapture != null) {
                    throw new ArgumentException("ScopedTo and FilenameRegexScopeCapture cannot both be non-null.");
                }
                this.CultureInfoString = readerConfiguration.ContainsKey("CultureInfo") ?
                    readerConfiguration["CultureInfo"] : null;
                this.Delimiter = readerConfiguration.ContainsKey("Delimiter") ?
                    readerConfiguration["Delimiter"] : ",";
            } catch (KeyNotFoundException) {
                string configStr = string.Join(",\n  ", this.GetType().GetProperties().ToList());
                string readerConfigString = string.Join(",\n  ", readerConfiguration.Select(x => x.Key + ": " + x.Value));
                string msg = $"OffsetTagsReader configuration requires the following fields to be set:\n  {configStr}" +
                             "\nHowever, only the following inputs were received:\n  " + readerConfigString;
                throw new ArgumentException(msg);
            }
        }
    }
}