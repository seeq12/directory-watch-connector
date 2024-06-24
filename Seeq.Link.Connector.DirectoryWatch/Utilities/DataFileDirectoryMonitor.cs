using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Seeq.Link.SDK.Interfaces;
using Seeq.Link.Connector.DirectoryWatch.Interfaces;
using System.IO;

namespace Seeq.Link.Connector.DirectoryWatch.Utilities {

    public class DataFileDirectoryMonitor : IFileChangeListener {

        private static readonly log4net.ILog log = log4net.LogManager.GetLogger
            (System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        public string Directory { get; set; }
        private Regex filenameRegex;
        private Regex subdirectoryRegex;
        private bool watchSubdirectories = false;
        private Dictionary<string, DirectoryWatcher> directoryWatchers;
        private DataFileReader dataFileReader;
        private TimeSpan changePollingInterval = TimeSpan.FromSeconds(5);
        private TimeSpan changeDebouncePeriod = TimeSpan.FromSeconds(1);
        private DirectoryInfo mainDirectoryInfo;
        private List<DirectoryInfo> directoryInfos;
        private int maxFilesPerDir;

        private readonly object lockObj = new object();

        public DataFileDirectoryMonitor(string directory, Regex filenameRegex, Regex subdirectoryRegex, bool watchSubdirectories, DataFileReader reader, TimeSpan changePollingInterval, TimeSpan changeDebouncePeriod, string maxFilesPerDir) {
            this.Directory = directory;
            this.filenameRegex = filenameRegex;
            this.subdirectoryRegex = subdirectoryRegex;
            this.watchSubdirectories = watchSubdirectories;
            this.directoryWatchers = new Dictionary<string, DirectoryWatcher>();
            this.dataFileReader = reader;
            this.changePollingInterval = changePollingInterval;
            this.changeDebouncePeriod = changeDebouncePeriod;
            this.maxFilesPerDir = Convert.ToInt32(maxFilesPerDir);

            this.directoryInfos = new List<DirectoryInfo>();
            this.mainDirectoryInfo = new DirectoryInfo(this.Directory);
        }

        private void directoryWatchAction(string directory) {
            Action<DirectoryInfo> readFilesInDirectory = (info) => {
                foreach (FileInfo fileInfo in info.GetFiles()) {
                    if (filenameRegex.IsMatch(fileInfo.Name)) {
                        try {
                            var originalFileName = fileInfo.FullName;
                            string modifiedFilename = Path.ChangeExtension(originalFileName, ".importing");
                            if (File.Exists(modifiedFilename)) {
                                File.Delete(modifiedFilename);
                            }
                            File.Move(originalFileName, modifiedFilename);
                            dataFileReader.ValidateAndReadFile(originalFileName, modifiedFilename);
                            string importedFilename = Path.ChangeExtension(modifiedFilename, ".imported");
                            if (File.Exists(importedFilename)) {
                                File.Delete(importedFilename);
                            }
                            File.Move(modifiedFilename, importedFilename);
                        } catch (Exception ex) {
                            log.Info($"ReadFile failed on file {fileInfo.FullName} due to exception {ex.Message}", ex);
                        }
                    }
                }
            };

            lock (this.lockObj) {
                if (this.watchSubdirectories) {
                    List<DirectoryInfo> newDirectoryInfos = new List<DirectoryInfo>();
                    newDirectoryInfos = this.getSubdirectoryInfos(mainDirectoryInfo)
                        .FindAll(x => this.subdirectoryRegex.IsMatch(x.FullName));
                    newDirectoryInfos.OrderBy(x => x.FullName);
                    if (newDirectoryInfos.Equals(this.directoryInfos) == false) {
                        // Add any new infos
                        foreach (DirectoryInfo info in newDirectoryInfos) {
                            if (this.directoryInfos.Find(x => x.FullName == info.FullName) == null) {
                                readFilesInDirectory(new DirectoryInfo(info.FullName));
                                this.directoryWatchers.Add(info.FullName, new DirectoryWatcher(info.FullName, this, this.changePollingInterval, this.changeDebouncePeriod));
                                this.directoryWatchers[info.FullName].Start();
                            }
                        }
                        // Remove any existing that aren't in the new infos list
                        foreach (DirectoryInfo info in this.directoryInfos) {
                            if (newDirectoryInfos.Find(x => x.FullName == info.FullName) == null) {
                                this.directoryWatchers[info.FullName].Stop();
                                this.directoryWatchers.Remove(info.FullName);
                            }
                        }
                        this.directoryInfos = newDirectoryInfos;
                    }
                }
                DirectoryInfo directoryInfo = new DirectoryInfo(directory);
                readFilesInDirectory(new DirectoryInfo(directoryInfo.FullName));
            }
        }

        public void Initialize() {
            if (this.watchSubdirectories) {
                this.directoryInfos = this.getSubdirectoryInfos(mainDirectoryInfo)
                    .FindAll(x => this.subdirectoryRegex.IsMatch(x.FullName));
                this.directoryInfos.OrderBy(x => x.Name);
            } else {
                this.directoryInfos.Add(mainDirectoryInfo);
            }

            foreach (DirectoryInfo directoryInfo in directoryInfos) {
                // validate that we are not exceeding the file count limit
                var directoryFileCount = directoryInfo.GetFiles().Length;

                if (directoryFileCount > this.maxFilesPerDir) {
                    log.ErrorFormat("Directory '{0}' has {1} files which exceeds the maximum allowed: {2}", directoryInfo.FullName, directoryFileCount, this.maxFilesPerDir);
                    throw new InvalidOperationException("The number of files in the directory being read exceeds configured limits");
                }
                
                this.directoryWatchAction(directoryInfo.FullName);
                this.directoryWatchers.Add(directoryInfo.FullName, new DirectoryWatcher(directoryInfo.FullName, this, this.changePollingInterval, this.changeDebouncePeriod));
                this.directoryWatchers[directoryInfo.FullName].Start();
            }
        }

        public void Stop() {
            foreach (DirectoryWatcher directoryWatcher in this.directoryWatchers.Values) {
                directoryWatcher.Stop();
            }
        }

        public void OnFileModify(string filePath) {
            this.directoryWatchAction(filePath);
        }

        public void OnFileDelete(string filePath) {
            this.directoryWatchAction(filePath);
        }

        private List<DirectoryInfo> getSubdirectoryInfos(DirectoryInfo directoryInfo) {
            List<DirectoryInfo> directoryInfos = new List<DirectoryInfo>();
            directoryInfos.Add(directoryInfo);
            foreach (DirectoryInfo subDirectoryInfo in directoryInfo.GetDirectories()) {
                directoryInfos = directoryInfos.Concat(getSubdirectoryInfos(subDirectoryInfo)).ToList();
            }
            return directoryInfos;
        }
    }
}