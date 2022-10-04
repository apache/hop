echo off
REM
REM Licensed to the Apache Software Foundation (ASF) under one or more
REM contributor license agreements.  See the NOTICE file distributed with
REM this work for additional information regarding copyright ownership.
REM The ASF licenses this file to You under the Apache License, Version 2.0
REM (the "License"); you may not use this file except in compliance with
REM the License.  You may obtain a copy of the License at
REM
REM       http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing, software
REM distributed under the License is distributed on an "AS IS" BASIS,
REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM See the License for the specific language governing permissions and
REM limitations under the License.
REM

echo off
setlocal

REM switch to script directory
cd /D %~dp0

REM Option to change the Characterset of the Windows Shell to show foreign caracters
if not "%HOP_WINDOWS_SHELL_ENCODING%"=="" chcp %HOP_WINDOWS_SHELL_ENCODING%

set LIBSPATH=lib
set SWTJAR=libswt\win64

set _temphelp=0
if [%1]==[help] set _temphelp=1
if [%1]==[Help] set _temphelp=1
if %_temphelp%==1 (GOTO Help) ELSE (GOTO NormalStart)

:Help
echo ===[Hop Help - hop-gui.bat]===============================================
echo Normally, no parameters are required to start Hop.  There is a debug mode
echo that you can start by passing in DEBUG as the first parameter after hop-gui.bat
echo.
echo Example:
echo   hop-gui.bat DEBUG
echo
echo The debug mode opens port 5005 locally when Hop starts allowing you to attaching
echo a debugger from your favorite Java IDE tool and step code.
echo ==========================================================================
GOTO End

:NormalStart
REM set java primary is HOP_JAVA_HOME fallback to JAVA_HOME or default java
if not "%HOP_JAVA_HOME%"=="" (
    set _HOP_JAVA="%HOP_JAVA_HOME%\bin\java"
) else if not "%JAVA_HOME%"=="" (
    set _HOP_JAVA="%JAVA_HOME%\bin\java"
) else (
    set _HOP_JAVA=java
)

REM # Settings for all OSses

if "%HOP_OPTIONS%"=="" set HOP_OPTIONS=-Xmx2048m

REM
REM If the user passes in DEBUG as the first parameter, it starts Hop in debugger mode and opens port 5005
REM to allow attaching a debugger to step code.
if [%1]==[DEBUG] (
REM # optional line for attaching a debugger
set HOP_OPTIONS=%HOP_OPTIONS% -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005)

REM Pass HOP variables if they're set.
if not "%HOP_AUDIT_FOLDER%"=="" (
  set HOP_OPTIONS=%HOP_OPTIONS% -DHOP_AUDIT_FOLDER=%HOP_AUDIT_FOLDER%
)
if not "%HOP_CONFIG_FOLDER%"=="" (
  set HOP_OPTIONS=%HOP_OPTIONS% -DHOP_CONFIG_FOLDER=%HOP_CONFIG_FOLDER%
)
if not "%HOP_SHARED_JDBC_FOLDER%"=="" (
  set HOP_OPTIONS=%HOP_OPTIONS% -DHOP_SHARED_JDBC_FOLDER=%HOP_SHARED_JDBC_FOLDER%
)
if not "%HOP_PLUGIN_BASE_FOLDERS%"=="" (
  set HOP_OPTIONS=%HOP_OPTIONS% -DHOP_PLUGIN_BASE_FOLDERS=%HOP_PLUGIN_BASE_FOLDERS%
)
if not "%HOP_PASSWORD_ENCODER_PLUGIN%"=="" (
  set HOP_OPTIONS=%HOP_OPTIONS% -DHOP_PASSWORD_ENCODER_PLUGIN=%HOP_PASSWORD_ENCODER_PLUGIN%
)
if not "%HOP_AES_ENCODER_KEY%"=="" (
  set HOP_OPTIONS=%HOP_OPTIONS% -DHOP_AES_ENCODER_KEY=%HOP_AES_ENCODER_KEY%
)

set HOP_OPTIONS=%HOP_OPTIONS% -DHOP_PLATFORM_OS=Windows
set HOP_OPTIONS=%HOP_OPTIONS% -DHOP_PLATFORM_RUNTIME=GUI

set HOP_OPTIONS=%HOP_OPTIONS% -DHOP_AUTO_CREATE_CONFIG=Y

echo ===[Environment Settings - hop-gui.bat]===================================
echo.
echo Java identified as %_HOP_JAVA%
echo.
echo HOP_OPTIONS=%HOP_OPTIONS%
echo.
echo Command to start Hop will be:
echo %_HOP_JAVA% -classpath %LIBSPATH%\*;%SWTJAR%\* -Djava.library.path=%LIBSPATH% %HOP_OPTIONS% org.apache.hop.ui.hopgui.HopGui
echo.
echo ===[Starting Hop]=========================================================

%_HOP_JAVA% -classpath %LIBSPATH%\*;%SWTJAR%\* -Dswt.autoScale=false -Djava.library.path=%LIBSPATH% %HOP_OPTIONS% org.apache.hop.ui.hopgui.HopGui
if ERRORLEVEL 1 (pause)
@echo off
:End