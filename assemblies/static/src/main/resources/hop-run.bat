set LIBSPATH=lib

@echo on
java %OPT%  -classpath %LIBSPATH%\* org.apache.hop.cli.HopRun
@echo off
