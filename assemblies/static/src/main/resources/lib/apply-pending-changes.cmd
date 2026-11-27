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

REM Applies the plugin file changes the Hop marketplace could not make while Hop was running
REM (plugins\.marketplace\pending). The Hop launchers call this before they start Java: once Java
REM runs, Windows keeps its jars locked.
REM
REM Usage, from the Hop installation folder:
REM   call lib\apply-pending-changes.cmd
REM
REM The list is applied in this folder and in the parent of every plugins folder named in
REM HOP_PLUGIN_BASE_FOLDERS, the places the marketplace installs into. Each line is one change:
REM   copy|<source>|<target>
REM   delete|<target>
REM Paths are relative to that folder and use /. A change that fails stays in the list for the
REM next start. Messages go to stderr.
REM
REM Lines are read with delayed expansion off and only used through !variables! after that, so
REM characters like | & ^ ! in a line are never taken as commands.

setlocal DisableDelayedExpansion

set "_hp_first=%CD%"
if "%_hp_first:~-1%"=="\" set "_hp_first=%_hp_first:~0,-1%"
call :apply_home "%CD%"

if not defined HOP_PLUGIN_BASE_FOLDERS goto :end
set "_hp_folders=%HOP_PLUGIN_BASE_FOLDERS%"

:next_folder
if not defined _hp_folders goto :end
set "_hp_rest=%_hp_folders%"
set "_hp_folders="
set "_hp_folder="
for /f "tokens=1,* delims=," %%a in ("%_hp_rest%") do (
  set "_hp_folder=%%a"
  set "_hp_folders=%%b"
)
if defined _hp_folder call :folder_home
goto :next_folder

:end
endlocal
exit /b 0


REM A HOP_PLUGIN_BASE_FOLDERS entry: the parent of a folder named plugins, or the folder itself.
:folder_home
for /f "tokens=*" %%f in ("%_hp_folder%") do set "_hp_folder=%%f"
REM D:\x\plugins\ names the plugins folder too, as it does for Hop: drop the trailing slash so
REM the name of the last folder is plugins, not empty.
:folder_home_trim
if not defined _hp_folder exit /b 0
if "%_hp_folder:~-1%"=="\" (set "_hp_folder=%_hp_folder:~0,-1%" & goto :folder_home_trim)
if "%_hp_folder:~-1%"=="/" (set "_hp_folder=%_hp_folder:~0,-1%" & goto :folder_home_trim)
for %%f in ("%_hp_folder%") do (
  if /i "%%~nxf"=="plugins" (
    call :apply_home "%%~dpf."
  ) else (
    call :apply_home "%%~ff"
  )
)
exit /b 0


:apply_home
set "_hp_home=%~f1"
if "%_hp_home:~-1%"=="\" set "_hp_home=%_hp_home:~0,-1%"
REM The installation folder itself comes first; skip it when a plugins folder points back to it.
if /i "%_hp_home%"=="%_hp_first%" if defined _hp_first_done exit /b 0
if /i "%_hp_home%"=="%_hp_first%" set "_hp_first_done=1"
set "_hp_list=%_hp_home%\plugins\.marketplace\pending"
if not exist "%_hp_list%" exit /b 0
REM A read-only installation: nothing can change here.
type nul > "%_hp_list%.remaining" 2>nul || exit /b 0

set /a _hp_applied=0
set /a _hp_failed=0
for /f "usebackq eol=# delims=" %%l in ("%_hp_list%") do (
  set "_hp_line=%%l"
  call :apply_line
  if errorlevel 1 (set /a _hp_failed+=1) else (call :mark_done)
)

REM Keep every line not handled here: a running Hop may have added lines in the meantime.
set /a _hp_kept=0
for /f "usebackq eol=# delims=" %%l in ("%_hp_list%") do (
  set "_hp_line=%%l"
  call :keep_if_not_done
)
if %_hp_kept% gtr 0 (
  move /y "%_hp_list%.remaining" "%_hp_list%" >nul 2>&1
) else (
  del /f /q "%_hp_list%.remaining" "%_hp_list%" >nul 2>&1
)

setlocal EnableDelayedExpansion
if !_hp_failed! gtr 0 (
  >&2 echo Hop marketplace: applied !_hp_applied! pending plugin changes in !_hp_home!, !_hp_failed! still waiting. Is Hop still running from this folder?
) else if !_hp_applied! gtr 0 (
  >&2 echo Hop marketplace: applied !_hp_applied! pending plugin changes in !_hp_home!
)
endlocal
exit /b 0


REM Remember a handled line in _hp_done_<n>.
:mark_done
set /a _hp_applied+=1
setlocal EnableDelayedExpansion
set "_hp_value=!_hp_line!"
for /f "eol=# delims=" %%v in ("!_hp_value!") do endlocal & set "_hp_done_%_hp_applied%=%%v"
exit /b 0


:keep_if_not_done
setlocal EnableDelayedExpansion
for /l %%i in (1,1,!_hp_applied!) do (
  if "!_hp_line!"=="!_hp_done_%%i!" exit /b 0
)
>>"!_hp_list!.remaining" echo(!_hp_line!
endlocal
set /a _hp_kept+=1
exit /b 0


REM Applies _hp_line. errorlevel 0: done with it (applied, or invalid and dropped). 1: try again at
REM the next start.
:apply_line
setlocal EnableDelayedExpansion
set "_hp_a="
set "_hp_b="
set "_hp_c="
for /f "tokens=1,2,* delims=|" %%a in ("!_hp_line!") do (
  set "_hp_a=%%a"
  set "_hp_b=%%b"
  set "_hp_c=%%c"
)

if "!_hp_a!"=="delete" (
  if not "!_hp_line!"=="delete|!_hp_b!" goto :apply_line_invalid
  set "_hp_p=!_hp_b!"
  call :valid_target || goto :apply_line_invalid
  set "_hp_t=!_hp_home!\!_hp_b:/=\!"
  call :is_directory && goto :apply_line_invalid
  if exist "!_hp_t!" del /f /q "!_hp_t!" >nul 2>&1
  if exist "!_hp_t!" (
    >&2 echo Hop marketplace: could not remove !_hp_b!, trying again at the next start
    exit /b 1
  )
  exit /b 0
)

if "!_hp_a!"=="copy" (
  if not "!_hp_line!"=="copy|!_hp_b!|!_hp_c!" goto :apply_line_invalid
  set "_hp_p=!_hp_b!"
  call :valid_source || goto :apply_line_invalid
  set "_hp_p=!_hp_c!"
  call :valid_target || goto :apply_line_invalid
  set "_hp_s=!_hp_home!\!_hp_b:/=\!"
  set "_hp_t=!_hp_home!\!_hp_c:/=\!"
  call :is_directory && goto :apply_line_invalid
  if not exist "!_hp_s!" (
    >&2 echo Hop marketplace: ignoring a pending change to !_hp_c!: !_hp_b! is gone
    exit /b 0
  )
  for %%d in ("!_hp_t!") do if not exist "%%~dpd" mkdir "%%~dpd" >nul 2>&1
  copy /y /b "!_hp_s!" "!_hp_t!" >nul 2>&1
  if errorlevel 1 (
    >&2 echo Hop marketplace: could not write !_hp_c!, trying again at the next start
    exit /b 1
  )
  del /f /q "!_hp_s!" >nul 2>&1
  exit /b 0
)

:apply_line_invalid
>&2 echo Hop marketplace: ignoring invalid pending change: !_hp_line!
exit /b 0


REM Path checks on _hp_p, the same the marketplace makes when it writes the list.
:valid_path
if not defined _hp_p exit /b 1
if "!_hp_p:~0,1!"=="/" exit /b 1
if "!_hp_p:~-1!"=="/" exit /b 1
if "!_hp_p:~0,2!"=="./" exit /b 1
if "!_hp_p:~-2!"=="/." exit /b 1
if not "!_hp_p::=!"=="!_hp_p!" exit /b 1
if not "!_hp_p:\=!"=="!_hp_p!" exit /b 1
if not "!_hp_p:..=!"=="!_hp_p!" exit /b 1
if not "!_hp_p://=!"=="!_hp_p!" exit /b 1
if not "!_hp_p:/./=!"=="!_hp_p!" exit /b 1
if not "!_hp_p:?=!"=="!_hp_p!" exit /b 1
REM !var:*=! means something else in cmd: look for * one character at a time.
for /l %%i in (0,1,300) do if "!_hp_p:~%%i,1!"=="*" exit /b 1
exit /b 0

:valid_target
call :valid_path || exit /b 1
if /i "!_hp_p:~0,21!"=="plugins/.marketplace/" exit /b 1
if /i "!_hp_p:~0,8!"=="plugins/" exit /b 0
if /i "!_hp_p:~0,4!"=="lib/" exit /b 0
if /i "!_hp_p:~0,25!"=="webapps/ROOT/WEB-INF/lib/" exit /b 0
exit /b 1

:valid_source
call :valid_path || exit /b 1
if /i not "!_hp_p:~0,35!"=="plugins/.marketplace/pending-files/" exit /b 1
set "_hp_n=!_hp_p:~35!"
if not "!_hp_n:/=!"=="!_hp_n!" exit /b 1
if /i not "!_hp_n:~-8!"==".pending" exit /b 1
exit /b 0

REM errorlevel 0 when _hp_t is a directory: del on a directory would empty it.
:is_directory
set "_hp_attributes="
for %%x in ("!_hp_t!") do set "_hp_attributes=%%~ax"
if "!_hp_attributes:~0,1!"=="d" exit /b 0
exit /b 1
