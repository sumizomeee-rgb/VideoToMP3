@echo off
chcp 65001 >nul
title VideoToMP3 Build
echo.
echo  Building VideoToMP3...
echo.
python "%~dp0build.py"
echo.
pause
