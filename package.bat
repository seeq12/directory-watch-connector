@echo off

call build.bat

powershell -NoProfile -ExecutionPolicy Bypass -File .\package.ps1