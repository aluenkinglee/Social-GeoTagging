# -*- coding: utf-8 -*-
import json
import os
import parseJson2Dict as p
from alchemytest import session,metadata,NearByInfo
import fnmatch
from datetime import datetime


    
def handler(path=os.getcwd()):
    cwd = path
    fs = os.listdir(cwd)
    total = 0
    for fn in fs:
        if  fnmatch.fnmatch ( fn, 'thread_*[0-9].json' ):
                print "now handling the file:",fn
                t = write_file_to_db(fn)
                total += t
    return total

def write_file_to_db(filename =""):
    cnt = 0 
    with open(filename) as f:
        for line in f:
            try:
                weibo_dict = json.loads(line)  
            except Exception, e:
                print "json decode error , it is a wrong format.",e
                continue
            
            # 将unicode编码转换成utf-8   
            wb = p.decode_dict(weibo_dict)
            del weibo_dict
            length = len( wb['statuses'])

            for i in range(length):
                if not wb['statuses'][i].has_key("user"):
                    print "this record is bad"
                elif  wb['statuses'][i]["user"]['geo_enabled']  :
                    if wb['statuses'][i].has_key("annotations"):
                        cnt = cnt + 1
                        if wb['statuses'][i]["annotations"][0].has_key("place"):
                            values = []
                            values.append(wb['statuses'][i]['text'])
                            dt1 = datetime.strptime(wb['statuses'][i]['created_at'], '%a %b %d %H:%M:%S +0800 %Y')
                            values.append(dt1)
                            values.append(wb['statuses'][i]['distance'])
                            values.append(wb['statuses'][i]['mid'])
                            values.append(wb['statuses'][i]["user"]['id'])
                            values.append(wb['statuses'][i]["user"]['city'])
                            values.append(wb['statuses'][i]["user"]['followers_count'])
                            values.append(wb['statuses'][i]["user"]['location'])
                            values.append(wb['statuses'][i]["user"]['type'])
                            values.append(wb['statuses'][i]["user"]['profile_url'])
                            values.append(wb['statuses'][i]["user"]['province'])
                            values.append(wb['statuses'][i]["user"]['description'])
                            values.append(wb['statuses'][i]["user"]['statuses_count'])
                            dt2 = datetime.strptime(wb['statuses'][i]["user"]['created_at'], '%a %b %d %H:%M:%S +0800 %Y')
                            
                            values.append(dt2)
                            values.append(wb['statuses'][i]["user"]['gender'])
                            values.append(wb['statuses'][i]["user"]['geo_enabled'])
                            values.append(wb['statuses'][i]["user"]['name'])

                            if  wb['statuses'][i]["annotations"][0]['place'].has_key("lat"):
                                values.append(wb['statuses'][i]["annotations"][0]['place']['lat'])
                                values.append(wb['statuses'][i]["annotations"][0]['place']['lon'])
                            elif wb['statuses'][i]["annotations"][0]['place'].has_key("latitude"):
                                values.append(wb['statuses'][i]["annotations"][0]['place']['latitude'])
                                values.append(wb['statuses'][i]["annotations"][0]['place']['longitude'])
                            else:
                                values.append(wb['statuses'][i]["geo"]["coordinates"][0])
                                values.append(wb['statuses'][i]["geo"]["coordinates"][1])

                            if wb['statuses'][i]["annotations"][0]['place'].has_key('title'):
                                values.append(wb['statuses'][i]["annotations"][0]['place']['title'])
                            else:
                                values.append("NO TITLE")

                            if wb['statuses'][i]["annotations"][0]['place'].has_key('poiid'):
                                values.append(wb['statuses'][i]["annotations"][0]['place']['poiid'])
                            else:
                                values.append("No POIID!!!!")

                            if wb['statuses'][i]["annotations"][0]['place'].has_key('type'):
                                values.append(wb['statuses'][i]["annotations"][0]['place']['type'])
                            else:
                                values.append("No type!!!!")

                            # 构造NearByInfo记录
                            nbi = NearByInfo(values)
                            # 避免插入重复数据
                            if not session.query(NearByInfo).filter(NearByInfo.mid.in_([nbi.mid])).first() :
                                session.add(nbi)
                                try:
                                    session.commit()
                                except Exception, e:
                                    session.rollback()
                                    # print "输入有误，不能写入数据库",e,
                                    # print nbi.text
                            # else:
                            #     print "已经写入数据库"   

                            del values
                            del nbi
            del wb
    return cnt
'''
# test the write_file_to_db
num = write_file_to_db("thread_1.json")   
print "处理了"+str(num)+"条数据"
'''
num = handler()
print "insert " ,num ,"records"