@echo off

set SEEQ_DW_CONNECTOR_HOME=%~dp0.

for /f "tokens=*" %%a IN ('dir /b *Seeq.Link.Connector*') DO set SEEQ_CONNECTOR_NAME=%%a

title Seeq Directory Watch Connector Dev Environment

echo.
echo Seeq Directory Watch Connector development environment is set up.
echo.
echo Your connector name is currently "%SEEQ_CONNECTOR_NAME%"
echo.
echo First, execute "build" to compile the connector.
echo Then execute "ide" to launch Visual Studio for development and debugging.
echo Finally, execute "package" to build a deployable connector package.
echo.
