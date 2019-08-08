import requests
import pandas as pd
from lxml import html
from zipfile import ZipFile
from io import BytesIO
from db import RedisHandler


class Downloader(object):

    def request_handler(self,url):
        response=requests.get(url)
        return response.status_code,response.content

    def lxml_parser(self,html_content,xpath):
        tree=html.fromstring(str(html_content))
        csv_links = tree.xpath(xpath)
        return csv_links

    def read_csv_file(self,zip_file,fname):
        df = pd.read_csv(zip_file.open(fname))
        final_filter=[]
        for x in fields_to_filter:
            for column in df.columns.tolist():
                if x in column.lower():
                    final_filter.append(column)
                    break
        return df,final_filter
    


if __name__ == "__main__":
    url="https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx"
    init=Downloader()
    status,html_content = init.request_handler(url)
    xpath='//*[@class="ullist"]//li/a[contains(@href,"Equity")]/@href'

    fields_to_filter=["code","name","open","high","low","close"]
    links = init.lxml_parser(html_content,xpath)
    all_data=[]
    for link in links:
        print(link)
        if not link.lower().endswith('zip'):
            continue
        try:
            status,html_content = init.request_handler(link)
            zip_file = ZipFile(BytesIO(html_content))
            files = zip_file.namelist()
            for fname in files:
                df,final_filter = init.read_csv_file(zip_file,fname)
                print(final_filter)
                
                df=df[final_filter]
                value=df["SC_NAME"].tolist()
                print(df.shape,len(value),len(set(value)))
                for index,row in df.iterrows():
                    dict_row=dict(row)
                    if dict_row in all_data:
                        continue
                    all_data.append(dict_row)
                    #print(dict_row)
                    #break
        except Exception as e:
            print(str(e))

    redis_init=RedisHandler()
    redis_init.load_bulk(all_data)
    
    print(len(all_data))
