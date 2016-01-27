@echo off
:loop
python apple_daily.py
python ltn.py
python tvbs.py
python yahoo.py
python shingle_news.py
python importance_sort.py
python sensitive_sort.py 
ping -n 3600 127.1>nul
goto loop  