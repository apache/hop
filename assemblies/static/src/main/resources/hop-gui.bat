set LIBSPATH=lib
set SWTJAR=libswt\win64

REM # Settings for all OSses

if "%HOP_OPTIONS%"=="" set HOP_OPTIONS="-Xmx2048m"

REM # optional line for attaching a debugger
set HOP_OPTIONS=%HOP_OPTIONS% "-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"


@echo on
java %OPT%  -classpath %LIBSPATH%\*;%SWTJAR%\* "-Djava.library.path=%LIBSPATH%" %HOP_OPTIONS%  org.apache.hop.ui.hopgui.HopGui
@echo off
