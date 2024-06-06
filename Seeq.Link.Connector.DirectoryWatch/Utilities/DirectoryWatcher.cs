using System;
using System.IO;
using System.Threading;
using Seeq.Link.SDK.Interfaces;

namespace Seeq.Link.Connector.DirectoryWatch.Utilities {

    /// <summary>
    /// Monitors a directory for changes to a specific directory and notifies the listener of any changes to the file.
    /// </summary>
    public class DirectoryWatcher {

        private static readonly log4net.ILog log = log4net.LogManager.GetLogger
            (System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private readonly string directory;
        private readonly IFileChangeListener listener;
        private TimeSpan pollInterval;
        private TimeSpan debouncePeriod;
        private FileSystemWatcher watcher;
        private Thread runnerThread;
        private readonly AutoResetEvent readyEvent = new AutoResetEvent(false);
        private readonly object lockObj = new object();
        private Checksum checksum;

        /// <summary>
        /// Constructor for the directory watcher. Note that {@link #start()} must be called to begin watching for
        /// changes to the directory.
        /// </summary>
        /// <param name="directory">The directory to watch for changes. Created if it does not exist.</param>
        /// <param name="listener">The listener that is invoked upon the directory being changed.</param>
        /// <param name="pollInterval">The rate at which the file system is polled for changes.</param>
        /// <param name="debouncePeriod">The period to wait before notifying the listener.</param>
        public DirectoryWatcher(string directory, IFileChangeListener listener, TimeSpan pollInterval, TimeSpan debouncePeriod) {
            this.directory = directory;
            this.listener = listener;
            this.runnerThread = null;
            this.pollInterval = pollInterval;
            this.debouncePeriod = debouncePeriod;
        }

        /// <summary>
        /// Start the directory watcher.
        /// </summary>
        /// <exception cref="System.IO.IOException">Thrown when the directory cannot be created or watched.</exception>
        public virtual void Start() {
            lock (this.lockObj) {
                if (this.runnerThread != null) {
                    return;
                }

                this.checksum = new Checksum(new DirectoryInfo(this.directory));

                Directory.CreateDirectory(this.directory);
                this.runnerThread = new Thread(this.run);
                this.runnerThread.Name = $"DirectoryWatcher: {Path.GetDirectoryName(this.directory)}";
                this.runnerThread.Start();
                try {
                    if (this.readyEvent.WaitOne(10000) == false) {
                        throw new IOException("Could not start DirectoryWatcher");
                    }
                } catch (ThreadInterruptedException) {
                    throw new IOException("DirectoryWatcher startup interrupted");
                }
            }
        }

        /// <summary>
        /// Stop this directory watcher.
        /// </summary>
        public virtual void Stop() {
            lock (this.lockObj) {
                if (this.runnerThread == null) {
                    return;
                }

                this.runnerThread.Interrupt();
                try {
                    this.runnerThread.Join();
                } catch (ThreadInterruptedException) {
                    // Do nothing, we are likely shutting down anyway
                }
                this.runnerThread = null;
            }
        }

        /// <summary>
        /// Determines whether or not the watcher service is running.
        /// </summary>
        /// <returns>True if the watcher is running, false otherwise</returns>
        public virtual bool IsRunning() {
            lock (this.lockObj) {
                return this.runnerThread != null;
            }
        }

        private long? changeWasDetectedAt = null;

        /// <summary>
        /// Creates a watch service on the directory and invokes the listeners when the watched directory changes.
        /// </summary>
        private void run() {
            log.Debug("DirectoryWatcher started");

            while (true) {
                try {
                    if (this.watcher == null) {
                        this.watcher = new FileSystemWatcher();
                        this.watcher.Path = this.directory;
                        this.watcher.NotifyFilter = NotifyFilters.LastAccess | NotifyFilters.LastWrite | NotifyFilters.FileName | NotifyFilters.DirectoryName;

                        // Add event handlers...
                        this.watcher.Changed += (sender, args) => { this.changeWasDetectedAt = Environment.TickCount; };
                        this.watcher.Created += (sender, args) => { this.changeWasDetectedAt = Environment.TickCount; };
                        this.watcher.Deleted += (sender, args) => { this.changeWasDetectedAt = Environment.TickCount; };
                        this.watcher.Renamed += (sender, args) => { this.changeWasDetectedAt = Environment.TickCount; };

                        this.watcher.EnableRaisingEvents = true;

                        this.readyEvent.Set();
                    }

                    if (this.changeWasDetectedAt.HasValue) {
                        long millisElapsed = Environment.TickCount - this.changeWasDetectedAt.Value;
                        if (millisElapsed > this.debouncePeriod.TotalMilliseconds) {
                            this.changeWasDetectedAt = null;

                            Checksum oldChecksum = this.checksum;
                            this.checksum = new Checksum(new DirectoryInfo(this.directory));

                            // Compare checksums, because it has proven possible for the filesystem to notify us of a change
                            // even though the directory is unchanged. This was seen at Tesla for the FileWatcher and was
                            // causing continual metadata syncs.
                            // Send callback on its own thread so that the FileWatcher can be stopped from within
                            // the callback itself
                            Thread callbackThread = new Thread(() => {
                                if (Directory.Exists(this.directory)) {
                                    this.listener.OnFileModify(this.directory);
                                } else {
                                    this.listener.OnFileDelete(this.directory);
                                }
                            });

                            callbackThread.Name = string.Format("DirectoryWatcher callback: {0}", Path.GetDirectoryName(this.directory));
                            callbackThread.Start();
                        }
                    }

                    Thread.Sleep((int)this.pollInterval.TotalMilliseconds);
                } catch (ThreadInterruptedException) {
                    log.Debug("DirectoryWatcher interrupted");
                    break;
                } catch (Exception e) {
                    log.Error("DirectoryWatcher encountered exception", e);
                }
            }

            if (this.watcher != null) {
                this.watcher.EnableRaisingEvents = false;
                this.watcher.Dispose();
                this.watcher = null;
            }

            log.Debug("DirectoryWatcher stopped");
        }
    }
}