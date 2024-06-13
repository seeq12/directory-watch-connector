# Overview

Welcome to the Seeq DirectoryWatch Connector. .

# Developing this Connector

This connector is to be developed on the Windows operating system. This section is broken into subsections that deal with prerequisites for and relevant information about how this connector can be extended, built, packaged and deployed. 

## Environment

This connector is written in C# and depends on and targets .NET Framework 4.8. If you do not have .NET Framework and need to install it, you may also need to restart your machine for the new version to take effect. If you are unsure what version of the .NET Framework you have installed, [this guide](https://learn.microsoft.com/en-us/dotnet/framework/migration-guide/how-to-determine-which-versions-are-installed) should be able to help.

All the commands and script in this guide are supposed to run from the 

## Integrated Development Environment (IDE)

Microsoft Visual Studio is the recommended tool for building and debugging this connector however other IDEs such as JetBrains Rider could be used if you and/or your team are more familiar with those. This connector has been built and tested using Microsoft Visual Studio 2022.

If you do not have Visual Studio or any alternative IDE installed, you can obtain a copy of the Community version of Visual Studio from this [link](https://visualstudio.microsoft.com/thank-you-downloading-visual-studio/?sku=Community&channel=Release&version=VS2022&source=VSLandingPage&cid=2030&passive=false).




NOTE: As of the R21.0.40.03 release, it is no longer necessary to replace the Seeq.Link.SDK whenever a new version of Seeq is installed or when first installing DirectoryWatch.

Steps to Install:
1. Shut down Seeq
2. If you already have a copy of DirectoryWatch installed, make a backup of $DATA\data\plugins\connectors\DirectoryWatch, where $DATA is your Seeq data folder.  The Seeq data folder is C:\ProgramData\Seeq by default.  If the DirectoryWatch folder doesn't exist, you'll need to create it first.
3. For a new installation, simply copy all of the contents of the Install Kit folder to the newly-created $DATA\data\plugins\connectors\DirectoryWatch folder.  An upgrade involves replacing the files and folders of the existing DirectoryWatch folder with the appropriate versions from the Install Kit.  For files in the Configurations folder, a comparison of the old and new versions is recommended to make sure any changes to the JSON schema are noted and adjusted as appropriate.  For FileReaders, the old versions of prepackaged readers should be replaced.  Custom readers may need to be updated to work correctly with the latest versions of the Seeq SDK and DirectoryWatch.
4. If you already have a copy of directory watch installed, make a backup of your current $DATA\data\configuration\link\DirectoryWatch Connector.json.
5. Place a copy of the included "DirectoryWatch Connector.JSON" file into the connector configuration folder, $DATA\data\configuration\link.  Make sure the lists of FileReaderFolders and ConfigurationFolders indicated in the configuration file correctly reference the locations of the folders where you have the readers and configurations.
6. Restart Seeq.

Steps to Test:
1. After restarting Seeq, you should begin to see messages in the net-link logs that contain the text "DirectoryWatch".  These will probably be errors on a new install, since the directories to be watched probably don't exist yet.
2. There should be at least three files in $DATA\data\plugins\connectors\DirectoryWatch\Configurations\Backups.  Open these and review the JSON configurations stored therein.  Copy any you would like to use for testing from the Backups folder to its parent, the Configurations folder.
3. Note in particular the FileDirectories list.  The list should contain fully-qualified paths to each directory to be watched by this configuration.  If referencing a network folder, you should use the UNC name, not the name starting with the letter of a mapped network drive.
4. Make a copy of the included TestData folder somewhere, and make sure the account running Seeq has read and write privileges to the folder.
5. Modify the FileDirectories lists in the configurations to reference the equivalent subfolder of TestData.  This could be something like "C:\\Users\\Batman\\Desktop\\TestData\\timestamp-asset-tags-csv\\ForteBio Files".  Note that each backslash in the file path has been doubled for proper JSON escaping.
6. No other modifications should be needed to the config files.  After saving the changes to the configurations, go back to $DATA\data\configuration\link\DirectoryWatch Connector.json", make a small change to the whitespace in the file, and save.  This will prompt a restart of the connector so that the changes to the config files get picked up (this may not be necessary in a later version).
7. After the connector restarts, the files should begin being read in.  After the read starts, the file ending will change to .importing.  Once the read has completed successfully, the file ending will change again, this time to .imported.  This is how you know the file was imported successfully.
8. If the file never changes to .imported, the reader had trouble reading the file.  This is obviously not what we want, but the readers are very particular by design, because not reading the file is better than reading it incorrectly and putting bad data into Seeq.  The .importing status is an indication that troubleshooting is needed, at which point the next step is to look at the logs.  If you test with the ForteBio data, you will notice that one of the files does in fact fail to import.  You can fix this by finding the associated error message in the net-link log and correcting the issue described there.
9. If the file never changes to .importing, there is probably something wrong with the configuration itself.  Check the JSON with a validator like jsonlint.com, and make sure the referenced directory is accessible.
10. Once some files have changed to .imported, you should be able to find the data in Seeq.  The existing readers put data into an Asset Tree based on the data found in the file or as part of the file's path in the file system.  If you are testing the timestamp-asset-tags-csv data, you should find an Asset Tree with root named "Projects" in the data panel.  If you are testing the timestamp-tags-csv data, you should look for a tree with root named "MyGroup".
11. In the DataFileReaders folder of the install kit, there is a directory for each Reader that comes with the installation.  Within the root folder for each Reader, there should be a README.txt file that describes how to configure the reader.

Custom Readers:

Custom readers can be placed anywhere the account running the .NET Agent (usually the account running Seeq Server) can find them; the containing folder for the Reader just has to be added to the FileReaderFolders list in DirectoryWatch Connector.json.  Once such a folder is listed in the connector configuration in such a way, the Reader dll located at ReferencedFileReaderFolder\SomeFileReader\bin\Debug or ReferencedFileReaderFolder\SomeFileReader\bin\Release will be dynamically loaded when the connector is started, depending on whether the connector configuration has DebugMode set to true or false.  A custom reader should only be defined if the existing Readers cannot be configured to suit the files to be read.  When such is the case, it is recommended to start with the existing Reader that has the greatest similarity to the Reader that is needed.  Each Reader must implement a constructor, an Initialize() method, and a ReadFile(string filename) method to satisfy the DirectoryWatchFileReader interface.  A few convenience methods like SendData and GetHeaderIndex are provided in Seeq.Link.DirectoryWatcy.Utilities.DirectoryWatchUtilities to make it easier to focus more on the details of how to read the file and less on how to get the data into Seeq, but there is no obligation to use these methods - the data can be posted directly using the Seeq SDK.