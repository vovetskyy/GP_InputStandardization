echo off

call _SET_ENV.cmd

rem %PYTHON_EXE_PATH%\python.exe %TOOL_STD_INPUT_SOURCES_PATH%\GP_StandardizeRawInput.py >norm.log 2>error.log
python.exe %TOOL_STD_INPUT_SOURCES_PATH%\GP_StandardizeRawInput.py >report.txt 2>exec.log