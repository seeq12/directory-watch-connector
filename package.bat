@echo off

call build.bat

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0.\package.ps1"
