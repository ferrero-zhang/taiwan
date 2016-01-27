@echo off

:loop
python prediction1130.py
ping -n 84000 127.1>nul

goto loop

