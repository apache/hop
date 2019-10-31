set LIBSPATH=lib
set SWTJAR=libswt\win64

@echo on
java %OPT%  -classpath %LIBSPATH%\*;%SWTJAR%\*  org.apache.hop.ui.hopui.HopUi
@echo off
