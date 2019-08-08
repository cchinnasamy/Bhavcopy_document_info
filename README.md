Deployed URL :
    http://13.232.15.183:8080
    
#cron schedule

crontab -e

0 8 * * * /usr/bin/python3.6 /opt/crawl/zerodha/Bhavcopy_document_info/crawler.py
