@echo off

call build.bat

set "ROOT_OUT_DIR=dist"
set "OUT_FILE=Seeq.Link.Connector.DirectoryWatch.zip"
set "READERS_OUT_DIR=DirectoryWatchFileReaders"
set "CONDITIONS_READER_OUT_DIR=%READERS_OUT_DIR%\ConditionsWithPropertiesReader"
set "NARROW_READER_OUT_DIR=%READERS_OUT_DIR%\NarrowFileReader"
set "OFFSET_READER_OUT_DIR=%READERS_OUT_DIR%\OffsetTagsReader"
set "TAGS_READER_OUT_DIR=%READERS_OUT_DIR%\TagsWithMetadataReader"
set "TS_ASSET_READER_OUT_DIR=%READERS_OUT_DIR%\TimestampAssetTagsCsvReaderV1"
set "TS_READER_OUT_DIR=%READERS_OUT_DIR%\TimestampTagsCsvReader"

set "READER_DIRS=%READERS_OUT_DIR% %CONDITIONS_READER_OUT_DIR% %NARROW_READER_OUT_DIR% %OFFSET_READER_OUT_DIR% %TAGS_READER_OUT_DIR% %TS_ASSET_READER_OUT_DIR% %TS_READER_OUT_DIR%"

echo Creating the output directory structure
rmdir /S /Q %ROOT_OUT_DIR%

mkdir "%ROOT_OUT_DIR%"
cd "%ROOT_OUT_DIR%"

for %%a in (%READER_DIRS%) do (
    mkdir "%%~fa\"
    echo Directory "%%~fa\" created
)

cd ..

echo Copying the source file readers
for %%d in (%READER_DIRS%) do (
    xcopy "%cd%\%%d\*.*" "%cd%\%ROOT_OUT_DIR%/%%d\" /I /Y
)

echo Copying the compiled libraries to the output directory
xcopy "Seeq.Link.Connector.DirectoryWatch/bin/Release\*" "%ROOT_OUT_DIR%\" /E /I /Y

powershell Compress-Archive -Path "%ROOT_OUT_DIR%\*" -DestinationPath "%ROOT_OUT_DIR%\%OUT_FILE%" -Force
echo Directory "%ROOT_OUT_DIR%" compressed into "%OUT_FILE%".

for /D %%d in ("%ROOT_OUT_DIR%\*") do (
    rmdir /S /Q "%%d"
)
for %%f in ("%ROOT_OUT_DIR%\*") do (
    if not "%%~nxf" == "%OUT_FILE%" (
        del /Q "%%f"
    )
)

echo DONE!!!