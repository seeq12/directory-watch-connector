@echo off

if defined SEEQ_DW_CONNECTOR_HOME goto :InDevEnvironment

echo.
echo You're not in the Connector SDK Dev Environment.
echo Execute 'environment' first.
echo.
exit /b 1
goto :EOF

:InDevEnvironment

"%~dp0.\Seeq.Link.Connector.DirectoryWatch.sln"
