@echo off
:loop

python alexa.py
    
python seo.py
    
python llchu.py
    
python soong.py
    
python tsaiingwen.py
    
python person.py
    
python llchu2015.py
    
python soong2015.py
    
python tsai2015.py
python llchu2016.py
    
python soong2016.py
    
python tsai2016.py

python fb_post_zan.py   
python fb_tsai.py
python fb_llchu.py
python fb_soong.py

ping -n 84000 127.1>nul

goto loop