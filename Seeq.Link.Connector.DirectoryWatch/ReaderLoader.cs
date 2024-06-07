using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Seeq.Link.Connector.DirectoryWatch.Interfaces;
using Seeq.Link.SDK.Utilities;

namespace Seeq.Link.Connector.DirectoryWatch {
    /// <summary>
    /// Loads any DLLs found on its ReaderSearchPaths properties that have classes that
    /// implement the <see cref="DataFileReader"/> interface.
    /// </summary>

    public class ReaderLoader {

        private static readonly log4net.ILog log = log4net.LogManager.GetLogger
            (System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        /// <summary>
        /// A list of file paths with a terminating file pattern that dictates
        /// where the loader will search for DLLs. Glob wildcards can be used.
        ///
        /// For example:
        /// ..\..\..\**\Seeq.Link.Connector.PI\*.dll
        ///
        /// Relative and absolute file paths can be supplied. If relative, the
        /// folder that the agent DLL is located in is used as the root.
        /// </summary>
        public string[] ReaderSearchPaths { get; private set; }

        public void Initialize(string[] readerSearchPaths) {
            this.ReaderSearchPaths = readerSearchPaths;
        }

        public Dictionary<string, Func<Dictionary<string, string>, object>> LoadDataFileReaderDLLs(bool debugMode = false) {
            // Grab the full path of the Agent DLL that is currently executing
            string executingAssemblyLocation = System.Reflection.Assembly.GetExecutingAssembly().Location;

            Dictionary<string, Func<Dictionary<string, string>, object>> readerInstantiators =
                new Dictionary<string, Func<Dictionary<string, string>, object>>();

            foreach (string searchPath in this.ReaderSearchPaths) {
                log.Info($"Searching for readers using path: '{searchPath}'");

                IEnumerable<string> readerDLLs = FileSystemGlob.Find(searchPath, Path.GetDirectoryName(executingAssemblyLocation));

                bool atLeastOneDllFound = false;
                foreach (string filePath in readerDLLs) {
                    atLeastOneDllFound = true;

                    log.Info($"Loading candidate DLL: '{filePath}'");

                    Assembly assembly = null;
                    try {
                        // Load the DLL into memory so that we can use reflection to find IDataFileReader classes
                        // Note that we use UnsafeLoadFrom() so that the load will work if the Seeq image has been downloaded from the web in a zip file
                        assembly = Assembly.UnsafeLoadFrom(filePath);
                    } catch (Exception e) {
                        log.Error($"Could not load assembly '{filePath}':\n{e.Message}", e);
                        continue;
                    }

                    bool atLeastOneTypeFound = false;
                    try {
                        foreach (Type type in assembly.GetExportedTypes()) {
                            if (type.IsAbstract == true) {
                                continue;
                            }

                            atLeastOneTypeFound = true;

                            // Add an instantiator for the reader to the dictionary of instantiators.
                            if (type.GetInterfaces().Contains(typeof(IDataFileReader))) {
                                try {
                                    readerInstantiators.Add(type.ToString().Split('.').Last(), x => Activator.CreateInstance(type, new object[] { x, debugMode }));
                                    log.Info($"Successfully added DataFileReader of type '{type.ToString()}'");
                                } catch (Exception e) {
                                    log.Error($"Could not instantiate class '{type}', exception:\n{e.Message}", e);
                                    continue;
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.Error($"Error loading reader '{filePath}':\n{e}", e);
                        continue;
                    }

                    if (atLeastOneTypeFound == false) {
                        log.Info($"No types found in DLL '{filePath}' that implement IDataFileReader interface");
                    }
                }

                if (atLeastOneDllFound == false) {
                    log.Info($"No matching DLLs found on search path '{searchPath}'");
                }
            }

            return readerInstantiators;
        }
    }
}