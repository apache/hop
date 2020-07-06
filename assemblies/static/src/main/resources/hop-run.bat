
set LIBSPATH=lib
set SWTJAR=libswt\win64

REM set java primary is HOP_JAVA_HOME fallback to JAVA_HOME or default java
if not "%HOP_JAVA_HOME%"=="" (
    set _HOP_JAVA="%HOP_JAVA_HOME%\bin\java"
) else if not "%JAVA_HOME%"=="" (
    set _HOP_JAVA="%JAVA_HOME%\bin\java"
) else (
    set _HOP_JAVA="java"
)

REM # Settings for all OSses

if "%HOP_OPTIONS%"=="" set HOP_OPTIONS="-Xmx2048m"

REM # optional line for attaching a debugger
set HOP_OPTIONS=%HOP_OPTIONS% "-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5006"

REM Pass HOP variables if they're set.
if not "%HOP_AUDIT_FOLDER%"=="" (
  set HOP_OPTIONS=%HOP_OPTIONS% "-DHOP_AUDIT_FOLDER="%HOP_AUDIT_FOLDER%
)
if not "%HOP_CONFIG_FOLDER%"=="" (
  set HOP_OPTIONS=%HOP_OPTIONS% "-DHOP_CONFIG_FOLDER="%HOP_CONFIG_FOLDER%
)
if not "%DHOP_SHARED_JDBC_FOLDER%"=="" (
  set HOP_OPTIONS=%HOP_OPTIONS% "-DHOP_SHARED_JDBC_FOLDER="%HOP_SHARED_JDBC_FOLDER%
)

set HOP_OPTIONS=%HOP_OPTIONS% "-DHOP_PLATFORM_OS=Windows"
set HOP_OPTIONS=%HOP_OPTIONS% "-DHOP_PLATFORM_RUNTIME=Run"

@echo on
%_HOP_JAVA% -classpath %LIBSPATH%\*;%SWTJAR%\* "-Djava.library.path=%LIBSPATH%" %HOP_OPTIONS% org.apache.hop.run.HopRun
@echo off
