@echo off

echo y|rmdir /s Seeq.Link.Connector.DirectoryWatch\bin 
echo y|rmdir /s Seeq.Link.Connector.DirectoryWatch\obj

echo y|rmdir /s .\DirectoryWatchFileReaders\ConditionsWithPropertiesReader\bin
echo y|rmdir /s .\DirectoryWatchFileReaders\ConditionsWithPropertiesReader\obj
echo y|rmdir /s .\DirectoryWatchFileReaders\NarrowFileReader\bin
echo y|rmdir /s .\DirectoryWatchFileReaders\NarrowFileReader\obj
echo y|rmdir /s .\DirectoryWatchFileReaders\OffsetTagsReader\bin
echo y|rmdir /s .\DirectoryWatchFileReaders\OffsetTagsReader\obj
echo y|rmdir /s .\DirectoryWatchFileReaders\TagsWithMetadataReader\bin
echo y|rmdir /s .\DirectoryWatchFileReaders\TagsWithMetadataReader\obj
echo y|rmdir /s .\DirectoryWatchFileReaders\TimestampAssetTagsCsvReaderV1\bin
echo y|rmdir /s .\DirectoryWatchFileReaders\TimestampAssetTagsCsvReaderV1\obj
echo y|rmdir /s .\DirectoryWatchFileReaders\TimestampTagsCsvReader\bin
echo y|rmdir /s .\DirectoryWatchFileReaders\TimestampTagsCsvReader\obj
