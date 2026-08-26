@echo off

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

setlocal EnableDelayedExpansion

REM switch to script directory
cd /D %~dp0

REM Optional user-level env written by `hop setup` (does not override already-set variables)
if exist "%USERPROFILE%\.hop\hop-env.cmd" call "%USERPROFILE%\.hop\hop-env.cmd"

REM Option to change the character set of the Windows Shell to show foreign characters
if not "%HOP_WINDOWS_SHELL_ENCODING%"=="" chcp %HOP_WINDOWS_SHELL_ENCODING%

set LIBSPATH=lib\core
set CLASSPATH=lib\core\*;lib\spark-client\*;lib\swt\win64\*

REM Optional versioned Spark client pack
if defined HOP_SPARK_CLIENT_VERSION if exist "lib\spark-clients\%HOP_SPARK_CLIENT_VERSION%\" (
  set CLASSPATH=%CLASSPATH%;lib\spark-clients\%HOP_SPARK_CLIENT_VERSION%\*
)





:NormalStart
REM set java primary is HOP_JAVA_HOME fallback to JAVA_HOME or default java
REM
if not "%HOP_JAVA_HOME%"=="" (
    set _HOP_JAVA="%HOP_JAVA_HOME%\bin\java"
) else if not "%JAVA_HOME%"=="" (
    set _HOP_JAVA="%JAVA_HOME%\bin\java"
) else (
    set _HOP_JAVA="java"
)

REM # Settings for all OSses
REM

REM HOP_OPTIONS is user input. The launcher accumulates its own flags in _HOP_OPTIONS so
REM the expanded value is never exported to the java process and fed back into the setup dialog.
set "_HOP_OPTIONS=%HOP_OPTIONS%"
if "%_HOP_OPTIONS%"=="" set _HOP_OPTIONS="-Xmx2048m"

REM See if we need to enable some remote debugging options for our developers.
REM
FOR %%a in (%*) DO (
  if "%%~a" == "--dev-debug" (
    set _HOP_OPTIONS=%_HOP_OPTIONS% -Xdebug -Xnoagent -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5010
  )
  if "%%~a" == "--dev-debug-wait" (
    set _HOP_OPTIONS=%_HOP_OPTIONS% -Xdebug -Xnoagent -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5010
  )
)

REM Pass HOP variables if they're set.
REM
if not "%HOP_AUDIT_FOLDER%"=="" (
  set _HOP_OPTIONS=%_HOP_OPTIONS% -DHOP_AUDIT_FOLDER="%HOP_AUDIT_FOLDER%"
) else (
   set _HOP_OPTIONS=%_HOP_OPTIONS% -DHOP_AUDIT_FOLDER=.\audit
)
if not "%HOP_CONFIG_FOLDER%"=="" (
  set _HOP_OPTIONS=%_HOP_OPTIONS% -DHOP_CONFIG_FOLDER="%HOP_CONFIG_FOLDER%"
)
if not "%HOP_SHARED_JDBC_FOLDERS%"=="" (
  set _HOP_OPTIONS=%_HOP_OPTIONS% -DHOP_SHARED_JDBC_FOLDERS="%HOP_SHARED_JDBC_FOLDERS%"
)
if not "%HOP_PLUGIN_BASE_FOLDERS%"=="" (
  set _HOP_OPTIONS=%_HOP_OPTIONS% -DHOP_PLUGIN_BASE_FOLDERS="%HOP_PLUGIN_BASE_FOLDERS%"
)
if not "%HOP_PASSWORD_ENCODER_PLUGIN%"=="" (
  set _HOP_OPTIONS=%_HOP_OPTIONS% -DHOP_PASSWORD_ENCODER_PLUGIN=%HOP_PASSWORD_ENCODER_PLUGIN%
)
if not "%HOP_AES_ENCODER_KEY%"=="" (
  set _HOP_OPTIONS=%_HOP_OPTIONS% -DHOP_AES_ENCODER_KEY=%HOP_AES_ENCODER_KEY%
)
if not "%HOP_AES_ENCODER_KEY_FILE%"=="" (
  set _HOP_OPTIONS=%_HOP_OPTIONS% -DHOP_AES_ENCODER_KEY_FILE=%HOP_AES_ENCODER_KEY_FILE%
)

set _HOP_OPTIONS=%_HOP_OPTIONS% -DHOP_PLATFORM_OS=Windows
set _HOP_OPTIONS=%_HOP_OPTIONS% -DHOP_PLATFORM_RUNTIME=Run
set _HOP_OPTIONS=%_HOP_OPTIONS% -DHOP_AUTO_CREATE_CONFIG=Y
set _HOP_OPTIONS=%_HOP_OPTIONS% --add-opens java.xml/jdk.xml.internal=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/sun.nio.cs=ALL-UNNAMED --add-opens java.base/sun.security.action=ALL-UNNAMED --add-opens java.base/sun.util.calendar=ALL-UNNAMED --add-opens java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED

REM ===[Collect command line arguments...]======================================
REM
set _cmdline=%*

:Run

%_HOP_JAVA% -classpath %CLASSPATH% -Djava.library.path=%LIBSPATH% %_HOP_OPTIONS% org.apache.hop.hop.Hop %_cmdline%%

