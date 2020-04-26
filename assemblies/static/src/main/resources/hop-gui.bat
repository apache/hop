set LIBSPATH=lib
set SWTJAR=libswt\win64

REM set java primary is HOP_JAVA_HOME fallback to JAVA_HOME or default java
if not "%HOP_JAVA_HOME%"=="" (
    set _HOP_JAVA=%HOP_JAVA_HOME%
) else if not "%JAVA_HOME%"=="" (
    set _HOP_JAVA=%JAVA_HOME%
) else (
    set _HOP_JAVA="java"
)

REM # Settings for all OSses

if "%HOP_OPTIONS%"=="" set HOP_OPTIONS="-Xmx2048m"

REM # optional line for attaching a debugger
set HOP_OPTIONS=%HOP_OPTIONS% "-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"

@echo on
%_HOP_JAVA% %OPT%  -classpath %LIBSPATH%\*;%SWTJAR%\* "-Djava.library.path=%LIBSPATH%" %HOP_OPTIONS%  org.apache.hop.ui.hopgui.HopGui
@echo off