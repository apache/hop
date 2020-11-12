@echo off
setlocal
set LIBSPATH=lib
set SWTJAR=libswt\win64

set _temphelp=0
if [%1]==[help] set _temphelp=1
if [%1]==[Help] set _temphelp=1
if %_temphelp%==1 (GOTO Help) ELSE (GOTO NormalStart)

:Help
echo ===[HopRun - hop-run.bat]=================================================
echo.
echo Example:
echo   hop-run.bat --file=C:\Users\usbra\Desktop\converted_financial_metrics\update_dashboard_financials.hwf --environment=converted_financial_metrics --project=converted_financial_metrics --runconfig=local
echo
echo The above gives a full path to a file. It assumes a project has been created for that file called 'converted_financial_metrics'
echo The project also has an environment called 'converted_financial_metrics', that was created when creating the project.
echo Finally, 'local' is the name of the local configuration json file present in the project's metadata folder.  There is one for workflows and another for pipelines.
echo ==========================================================================
GOTO End

:NormalStart
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

REM
REM If the user passes in DEBUG as the first parameter, it starts Hop in debugger mode and opens port 5005
REM to allow attaching a debugger to step code.
if [%1]==[DEBUG] (
REM # optional line for attaching a debugger
set HOP_OPTIONS=%HOP_OPTIONS% -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005)

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

set HOP_OPTIONS=%HOP_OPTIONS% -DHOP_PLATFORM_OS=Windows
set HOP_OPTIONS=%HOP_OPTIONS% -DHOP_PLATFORM_RUNTIME=Run

set HOP_OPTIONS=%HOP_OPTIONS% -DHOP_AUTO_CREATE_CONFIG=Y

echo ===[Environment Settings - hop-run.bat]===================================
echo.
echo Java identified as %_HOP_JAVA%
echo.
echo HOP_OPTIONS=%HOP_OPTIONS%
echo.
rem ===[Collect command line arguments...]======================================
set _cmdline=
:TopArg
if %1!==! goto EndArg
set _cmdline=%_cmdline% %1
shift
goto TopArg
:EndArg

echo.
echo Consolidated parameters to pass to HopRun are
echo %_cmdline%%
echo.
echo Command to start HopRun will be:
echo %_HOP_JAVA% -classpath %LIBSPATH%\*;%SWTJAR%\* -Djava.library.path=%LIBSPATH% %HOP_OPTIONS% org.apache.hop.run.HopRun %_cmdline%%
echo.
echo ===[Starting HopRun]=========================================================
%_HOP_JAVA% -classpath %LIBSPATH%\*;%SWTJAR%\* -Djava.library.path=%LIBSPATH% %HOP_OPTIONS% org.apache.hop.run.HopRun %_cmdline%%
@echo off
:End