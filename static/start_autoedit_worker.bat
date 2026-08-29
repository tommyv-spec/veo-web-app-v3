@echo off
REM Auto-edit worker launcher (LOCAL ONLY - never on Render).
REM The platform only queues auto-edit jobs; this PC does the rendering.
REM Nothing renders while this is not running.
REM
REM Start by hand:      double-click this file
REM Starts at logon:    registered as Scheduled Task "KavenoAutoEditWorker"
REM Remove autostart:   schtasks /delete /tn KavenoAutoEditWorker /f
REM Log:                %USERPROFILE%\.kaveno\autoedit_worker.log

setlocal
set REPO=c:\Users\tomma\Documents\Videos Obsidian 2
set LOGDIR=%USERPROFILE%\.kaveno
set LOG=%LOGDIR%\autoedit_worker.log
set PYTHONIOENCODING=utf-8

if not exist "%LOGDIR%" mkdir "%LOGDIR%"
cd /d "%REPO%"

:loop
echo [%date% %time%] starting auto-edit worker >> "%LOG%"
python -u "code\static\autoedit_worker.py" --watch --interval 15 >> "%LOG%" 2>&1
echo [%date% %time%] worker exited (code %errorlevel%) - restarting in 30s >> "%LOG%"
REM A crash must not stop the queue from draining: wait, then restart.
timeout /t 30 /nobreak >nul
goto loop
