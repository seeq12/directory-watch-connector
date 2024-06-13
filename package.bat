@echo off

call build.bat --all

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0.\package.ps1"
