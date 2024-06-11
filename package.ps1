# Define variables
$ROOT_OUT_DIR = "dist"
$OUT_FILE = "Seeq.Link.Connector.DirectoryWatch.zip"
$DOCS_OUT_DIR = Join-Path $ROOT_OUT_DIR "Docs"
$CONFIG_OUT_DIR = Join-Path $ROOT_OUT_DIR "Configuration"

$READERS_IN_DIR = "DirectoryWatchFileReaders"
$READERS_OUT_DIR = Join-Path $ROOT_OUT_DIR "DataFileReaders"

$CONDITIONS_READER_DIR = "ConditionsWithPropertiesReader"
$NARROW_READER_DIR = "NarrowFileReader"
$OFFSET_READER_DIR = "OffsetTagsReader"
$TAGS_READER_DIR = "TagsWithMetadataReader"
$TS_ASSET_READER_DIR = "TimestampAssetTagsCsvReaderV1"
$TS_READER_DIR = "TimestampTagsCsvReader"

$READER_DIRS = @($CONDITIONS_READER_DIR, $NARROW_READER_DIR, $OFFSET_READER_DIR, $TAGS_READER_DIR, $TS_ASSET_READER_DIR, $TS_READER_DIR)

Write-Output "Creating the output directory structure"

# Remove the existing directory
if (Test-Path $ROOT_OUT_DIR) {
    Remove-Item -Recurse -Force $ROOT_OUT_DIR
}

# Create the root, docs, config and readers output directories
New-Item -ItemType Directory -Path $ROOT_OUT_DIR
New-Item -ItemType Directory -Path $DOCS_OUT_DIR
New-Item -ItemType Directory -Path $CONFIG_OUT_DIR
New-Item -ItemType Directory -Path $READERS_OUT_DIR

# Create data file reader output subdirectories
foreach ($dir in $READER_DIRS) {
    $readerDir = Join-Path $READERS_OUT_DIR ("$dir\bin")
    New-Item -ItemType Directory -Path (Join-Path $readerDir "Debug")
    New-Item -ItemType Directory -Path (Join-Path $readerDir "Release")
    Write-Output "Directory $dir created"
}

# Copy the data file readers
Write-Output "Copying the data file readers"
foreach ($dir in $READER_DIRS) {
    $source = Join-Path $READERS_IN_DIR $dir
    $destination = Join-Path $READERS_OUT_DIR $dir
    
    Copy-Item -Path "$source\bin\Debug\*Reader.*" -Destination "$destination\bin\Debug"
    Copy-Item -Path "$source\bin\Release\*Reader.*" -Destination "$destination\bin\Release"
}

Write-Output "Copying the connector libraries"
$connectorSource = Join-Path (Get-Location) "Seeq.Link.Connector.DirectoryWatch\bin\Release\Seeq.Link.Connector.DirectoryWatch.*"
$connectorDestination = Join-Path $ROOT_OUT_DIR "\"
Copy-Item -Path $connectorSource -Destination $connectorDestination

# Compress the output directory into a zip file
Compress-Archive -Path (Join-Path $ROOT_OUT_DIR "*") -DestinationPath (Join-Path $ROOT_OUT_DIR $OUT_FILE) -Force
Write-Output "Directory $ROOT_OUT_DIR compressed into $OUT_FILE."

# Remove all directories in the output directory
Get-ChildItem -Path $ROOT_OUT_DIR -Directory | ForEach-Object {
    Remove-Item -Recurse -Force $_.FullName
}

# Remove all files except the output zip file
Get-ChildItem -Path $ROOT_OUT_DIR -File | ForEach-Object {
    if ($_.Name -ne $OUT_FILE) {
        Remove-Item -Force $_.FullName
    }
}

Write-Output "DONE!!!"
