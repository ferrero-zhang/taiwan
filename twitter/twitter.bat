@echo off
:loop
scrapy crawl twitter_search_web --loglevel=INFO --logfile=twitter.log
goto loop