@echo off

if not exist ".\nuget.exe" powershell -Command "(new-object System.Net.WebClient).DownloadFile('https://dist.nuget.org/win-x86-commandline/latest/nuget.exe', '.\nuget.exe')"

.\nuget install Seeq.Link.Connector.DirectoryWatch\packages.config -o packages
.\nuget install DirectoryWatchFileReaders\ConditionsWithPropertiesReader\packages.config -o packages
.\nuget install DirectoryWatchFileReaders\NarrowFileReader\packages.config -o packages
.\nuget install DirectoryWatchFileReaders\OffsetTagsReader\packages.config -o packages
.\nuget install DirectoryWatchFileReaders\TagsWithMetadataReader\packages.config -o packages
.\nuget install DirectoryWatchFileReaders\TimestampAssetTagsCsvReaderV1\packages.config -o packages
.\nuget install DirectoryWatchFileReaders\TimestampTagsCsvReader\packages.config -o packages

for /f "tokens=*" %%i in ('"%ProgramFiles(x86)%\Microsoft Visual Studio\Installer\vswhere.exe" -latest -products * -requires Microsoft.Component.MSBuild -find MSBuild\**\Bin\MSBuild.exe') do set MSBUILD_PATH=%%i
"%MSBUILD_PATH%" "%~dp0.\Seeq.Link.Connector.DirectoryWatch.sln" /p:Configuration="Release"
if ERRORLEVEL 1 goto :Error

goto :EOF

:Error
exit /b 1