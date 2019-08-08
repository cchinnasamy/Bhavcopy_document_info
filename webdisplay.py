import cherrypy
import redis
from mako.template import Template
import json
from db import RedisHandler

class Bhavcopy(object):
    
    @cherrypy.expose
    def index(self,get_name=None):
        
        init_redis=RedisHandler()
        filter_key="*"
        if get_name and get_name.strip():
            filter_key="*"+get_name.strip().upper()+"*"
            
        keys=init_redis.get_all_keys(filter_key)
            
        data=[]
        count=0
        for index,key in enumerate(keys):
            value=init_redis.hmgetall_get(key)
            if value:
                count+=1
                temp={}
                for key,value in value.items():
                    temp[key.decode('utf8')]=value.decode('utf8')
                data.append(temp)
            if count==10:
                break
        
        return Template(filename='table_html.html').render(data=data,columns=['SC_NAME','SC_CODE', 'OPEN', 'HIGH', 'LOW', 'CLOSE'])
cherrypy.server.socket_host = '0.0.0.0'
cherrypy.quickstart(Bhavcopy())
