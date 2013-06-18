# -*- coding: utf-8 -*-
from datetime import datetime
from weibo import APIClient
import Queue
import json
import threading
import time
import os

APP_KEY = '3386525732'  
APP_SECRET = 'c31c63155298bcd2ea16c2569b7fe3ca'  
CALLBACK_URL = 'http://www.aluenkinglee.com'  
access_token = '2.00D6kD4CmCWLhDa594bbaaf2uEiWmC'
expires_in = '1520989763'
client = APIClient(app_key=APP_KEY,app_secret=APP_SECRET,redirect_uri=CALLBACK_URL) 
client.set_access_token(access_token,expires_in)

location_config=(
    ('39.9843','116.3098'),('39.9907','116.2808'),('39.9984','116.3116'),
    ('39.9980','116.3529'),('39.9972','116.3899'),('39.9931','116.3366'),
    ('39.9829','116.3146'),('39.9808','116.3512'),('39.9805','116.3792'),
    ('39.9599','116.2982'),('39.9593','116.3230'),('39.9581','116.3546'),
    ('39.9681','116.3539'),('39.9674','116.3742'),('39.9685','116.3938'),
    )

queue = Queue.Queue()

def log(thread_num= None,location= None,):
    try:
        f = open("log.txt","a+")
    except Exception, e:
        print "Error in open log.txt:" + e
    log_info = u"thread %d is fetching data in %s  %s\n"  % (thread_num,str(location),datetime.now(),)
    print log_info
    f.write(log_info)
    f.close()

def _print_list(data):
    print '[',
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        print str(item) +',',
        if isinstance(item, list):
            item = _print_list(item)
        if isinstance(item, dict):
            item = _print_dict(item)
    print ']',

def _print_dict(data):
    print '{',
    for key, value in data.iteritems():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        key_str = '\"' +str(key) +'\":'
        print key_str,
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        else:
            if isinstance(value,str) :
                print '\"'+value+'\",',
            elif  isinstance(value,int):
                print value,', ',
            elif type(value) == type(None):
                print "None,",
        if isinstance(value, list):
            value = _print_list(value)
        elif isinstance(value, dict):
            value = _print_dict(value)
    print '}',

def println_list(data):
    _print_list(data)
    print '\n',

def println_dict(data):
    _print_dict(data)
    print '\n',

def _decode_list(data):
    rv = []
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        elif isinstance(item, list):
            item = _decode_list(item)
        elif isinstance(item, dict):
            item = _decode_dict(item)
        rv.append(item)
    return rv

def _decode_dict(data):
    rv = {}
    for key, value in data.iteritems():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        elif isinstance(value, list):
            value = _decode_list(value)
        elif isinstance(value, dict):
            value = _decode_dict(value)
        rv[key] = value
    return rv


def _decode_list_and_write(data,f):
    f.write("[")
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')        
        if isinstance(item, list):
            item = _decode_list_and_write(item,f)
        if isinstance(item, dict):
            item = _decode_dict_and_write(item,f)
        # if item == None:
        #    f.write("None")
        else :
            # print item
            f.write(str(item))

        f.write(',')
    f.write(']')

def _decode_dict_and_write(data,f):
    f.write("{")
    for key, value in data.iteritems():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        key_str = '\"' +str(key) +'\":'
        f.write(key_str)

        if isinstance(value, unicode):
            value = value.encode('utf-8')
        else:
            if isinstance(value,str) :
                f.write('\"')
                f.write(value)
                f.write('\", ')
            elif  isinstance(value,int):
                f.write(str(value))
                f.write(', ')
            elif type(value) == type(None):
                f.write("None,")
        if isinstance(value, list):
            value = _decode_list_and_write(value,f)
        elif isinstance(value, dict):
            value = _decode_dict_and_write(value,f)
    f.write('}')

def write_dict(data,f):
    _decode_dict_and_write(data,f)
    f.write('\n')

class ThreadNearby(threading.Thread):

    """Threaded nearby informations Grab"""
    def __init__(self, num,queue):
        #thread_num����̺߳ţ�queue���̹߳����
        threading.Thread.__init__(self)
        self.thread_num = num
        self.queue = queue
    

    def run(self):
        while True:
            #get location from queue
            location = self.queue.get()
            log(self.thread_num,location)

            #grabs nearby informations of location  return dict object
            try:
                weibo_dict = client.post.place__nearby_timeline(lat=location[0],long=location[1],count = 50)
            except IOError, e:
                print "Error to get weibo_dict object :" , e

            #data =  json.dumps(dict(json_object),indent=4)
            #print weibo_dict

            #append the data to file "thread_i.json"
            filename_json = "thread_" + str(self.thread_num) + ".json"
            filename_4json = "thread_" + str(self.thread_num) + "_beatiful.json"
            filename_dict = "thread_" + str(self.thread_num) + ".dict"

            wd = _decode_dict(weibo_dict)


            with open(filename_json,"a+") as f:
                print u"open " + filename_json + " to append data"
                json.dump(wd,f)
                f.write("\n")
            with open(filename_4json,"a+") as f:
                print u"open " + filename_4json + " to append data"
                json.dump(wd,f,indent = 4)
                f.write('\n')
            with open(filename_dict,"a+") as f:
                print u"open " + filename_dict + " to append data"
                write_dict(wd,f)

            '''
            跟上面的工作一样
            try:
                f = open (filename,"a+")
                print u"open " + filename + " to append data"
            except IOError,e:
                print ur"cant open file :%s and write" % (filename)
            f.write(data)
            f.write("\n")
            print u"write data to " + filename 
            f.close()
            '''
            del weibo_dict
            del location
            #signals to queue job is done
            self.queue.task_done()
            
def grab_information():
    num = len(location_config)

    # create thread and begin to grab info
    for i in range(num):
        tn = ThreadNearby(i,queue)
        tn.setDaemon(True)
        tn.start()

    #populate queue with data
    for location in location_config:
        queue.put(location)

    #wait on the queue until everything has been processed
    queue.join()

def grab_nearby_info():
    cwd = os.getcwd()
    dataspace = cwd + "\\nearby_info"
    try:
        os.makedirs(dataspace)
    except Exception, e:
        print "it exists."
    
    while  True:
        interval = 600
        grab_information()  
        time.sleep(interval)
 
def grab_user_checkin_info():
    uid_list=[]
    cwd = os.getcwd()

    with open ('uid.txt',"r") as f:
        for line in f:
            uid_list.append(line.split()[0]) 

    length = len(uid_list)

    for i in range(length):    
        t = cwd +"\\" +uid_list[i]
        os.makedirs(t)
        os.chdir(t)

        filename_json = uid_list[i] + ".json"
        filename_4json = uid_list[i] + "_beatiful.json"
        filename_dict = uid_list[i] + ".dict"
        _uid = uid_list[i]

        user_checkins = client.post.place__users__checkins(uid=_uid)

        if len(user_checkins) == 0:
            print "user:"+_uid+" has no checkins"
            total_number = 0
            with open(filename_json,"a+") as f:
                temp = {}
                json.dump(temp,f)
                f.write('\n')
            with open(filename_4json,"a+") as f:
                temp = {}
                json.dump(temp,f)
                f.write('\n')
            with open(filename_dict,"a+") as f:
                f.write("{}\n")
        else:
            #print isinstance(user_checkins,dict)
            uc = _decode_dict(user_checkins)
            #this is print to test
            #println_dict(uc)

            total_number = user_checkins["total_number"]
            _total_number = int (total_number)
            _count = 50
            _page = 1
            _PN = _total_number /_count + 1

            print "user:"+_uid+" has %s checkins,%d pages" % (total_number ,_PN)
            for i in  range(_PN):
                user_checkins = client.post.place__users__checkins(uid=_uid,count=_count,page=_page+i)
                print "now fetching page %d" %(_page+i)
                with open(filename_json,"a+") as f:
                    json.dump(uc,f)
                    f.write('\n')
                with open(filename_4json,"a+") as f:
                    json.dump(uc,f,indent = 4)
                    f.write('\n')
                with open(filename_dict,"a+") as f:
                    write_dict(uc,f)
        time.sleep(60)
        os.chdir(cwd)

def test_checkin():
    grab_user_checkin_info()  
    with open("1147735537.json","r") as f:
        for line in f:
            print type(line)
            print line,
            dd = json.loads(line)
            print type(dd),
            print len(dd['pois'])
    pass

if __name__ == '__main__':  
    #grab_user_checkin_info()
    grab_nearby_info()
