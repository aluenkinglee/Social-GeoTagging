# -*- coding: utf-8 -*-
import json
import parseJson2Dict as p
from alchemytest import session,metadata,NearByInfo


def write_file_to_db(filename =""):
    cnt = 0 
    with open(filename) as f:
        for line in f:
            weibo_dict = json.loads(line)  
            # 讲unicode编码转换成utf-8   
            wb = p.decode_dict(weibo_dict)
            del weibo_dict
            
            length = len( wb['statuses'])

            for i in range(length):
                if not wb['statuses'][i].has_key("user"):
                    print "this record is bad"
                elif  wb['statuses'][i]["user"]['geo_enabled']  :
                    if wb['statuses'][i].has_key("annotations"):
                        cnt = cnt + 1
                
                        # print 'text',wb['statuses'][i]['text']
                        # print 'created_at',wb['statuses'][i]['created_at']
                        # print 'distance',wb['statuses'][i]['distance']
                        # print 'mid',wb['statuses'][i]['mid']
                        # print 'user_id',wb['statuses'][i]["user"]['id']
                        # print 'user_city',wb['statuses'][i]["user"]['city']
                        # print 'user_followers_count',wb['statuses'][i]["user"]['followers_count']
                        # print 'user_location',wb['statuses'][i]["user"]['location']
                        # print 'user_type',wb['statuses'][i]["user"]['type']
                        # print 'user_profile_url',wb['statuses'][i]["user"]['profile_url']
                        # print 'user_province',wb['statuses'][i]["user"]['province']
                        # print 'user_description',wb['statuses'][i]["user"]['description']
                        # print 'user_statuses_count',wb['statuses'][i]["user"]['statuses_count']
                        # print 'user_created_at',wb['statuses'][i]["user"]['created_at']
                        # print 'user_gender',wb['statuses'][i]["user"]['gender']
                        # print 'user_geo_enabled',wb['statuses'][i]["user"]['geo_enabled']
                        # print 'user_geo_name',wb['statuses'][i]["user"]['name']

                        if wb['statuses'][i]["annotations"][0].has_key("place"):
                            values = []
                            values.append(wb['statuses'][i]['text'])
                            values.append(wb['statuses'][i]['created_at'])
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
                            values.append(wb['statuses'][i]["user"]['created_at'])
                            values.append(wb['statuses'][i]["user"]['gender'])
                            values.append(wb['statuses'][i]["user"]['geo_enabled'])
                            values.append(wb['statuses'][i]["user"]['name'])
                            if  wb['statuses'][i]["annotations"][0]['place'].has_key("lat"):
                                values.append(wb['statuses'][i]["annotations"][0]['place']['lat'])
                                values.append(wb['statuses'][i]["annotations"][0]['place']['lon'])
                                # print 'place_lat',wb['statuses'][i]["annotations"][0]['place']['lat']
                                # print 'place_lon',wb['statuses'][i]["annotations"][0]['place']['lon']
                            elif wb['statuses'][i]["annotations"][0]['place'].has_key("latitude"):
                                values.append(wb['statuses'][i]["annotations"][0]['place']['latitude'])
                                values.append(wb['statuses'][i]["annotations"][0]['place']['longitude'])
                                # print 'place_latitude',wb['statuses'][i]["annotations"][0]['place']['latitude']
                                # print 'place_longitude',wb['statuses'][i]["annotations"][0]['place']['longitude']
                            else:
                                values.append(wb['statuses'][i]["geo"]["coordinates"][0])
                                values.append(wb['statuses'][i]["geo"]["coordinates"][1])
                                # print "geo_lat",wb['statuses'][i]["geo"]["coordinates"][0]
                                # print "geo_lon",wb['statuses'][i]["geo"]["coordinates"][1]
                            if wb['statuses'][i]["annotations"][0]['place'].has_key('title'):
                                values.append(wb['statuses'][i]["annotations"][0]['place']['title'])
                                # print 'place_title',wb['statuses'][i]["annotations"][0]['place']['title']
                            else:
                                values.append("NO TITLE")
                                # print 'place_title',"he write nothing!!!!"
                            if wb['statuses'][i]["annotations"][0]['place'].has_key('poiid'):
                                values.append(wb['statuses'][i]["annotations"][0]['place']['poiid'])
                                # print 'place_poiid',wb['statuses'][i]["annotations"][0]['place']['poiid']
                            else:
                                values.append("No POIID!!!!")
                                # print 'place_poiid',"No POIID!!!!"
                            if wb['statuses'][i]["annotations"][0]['place'].has_key('type'):
                                values.append(wb['statuses'][i]["annotations"][0]['place']['type'])
                                # print 'place_type',wb['statuses'][i]["annotations"][0]['place']['type']
                            else:
                                values.append("No type!!!!")
                                # print 'place_type',"No type!!!!"
                            # 构造NearByInfo记录
                            nbi = NearByInfo(values)
                            # 避免插入重复数据
                            if not session.query(NearByInfo).filter(NearByInfo.mid.in_([nbi.mid])).first() :
                                session.add(nbi)
                                try:
                                    session.commit()
                                except Exception, e:
                                    session.rollback()
                                    print "输入有误，不能写入数据库",nbi.text
                            else:
                                print "已经写入数据库"   

                            del values
                            del nbi
            del wb
    return cnt

num = write_file_to_db("thread_1.json")   
print "处理了"+str(num)+"条数据"