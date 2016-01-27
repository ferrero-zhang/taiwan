@echo off

:loop
python shingle_twitter.py
python importance_sort_twitter.py
python sensitive_sort_twitter.py
python abnormal_v3.py
ping -n 3600 127.1>nul

goto loop

