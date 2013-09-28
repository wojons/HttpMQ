import re
import time
import hashlib
import random

import tornado.ioloop
import tornado.web
from tornado.escape import json_encode, json_decode
import torndb

class queueDB():
    def __init__(self):
        self.db = torndb.Connection("localhost", "tubes", user="root", password="sql")
    
    def setQueue(self, name):
        self.db = torndb.Connection("localhost", "tubes", user="root", password="sql")
        self.tube_name = name
        
    def claimJobs(self, key_hash, limit=1, ttr=60, ts=time.time()):
        cursor = self.db._cursor()
        cursor.execute("UPDATE `"+re.escape(self.tube_name)+"` SET key_hash=%s, state=1, ts=%s, ttr=%s WHERE (state=0 and ts < %s) OR (state=1 and ts+ttr < %s) ORDER BY jobId ASC LIMIT %s", [key_hash, ts, ttr, ts, ts, limit,])
        count = cursor.rowcount
        cursor.close()
        if count > 0:
            tasks, cursor = list(), self.db._cursor()
            cursor.execute("SELECT task,ts,jobId,ttr FROM `"+re.escape(self.tube_name)+"` WHERE key_hash=%s AND ts=%s ORDER BY jobId ASC LIMIT %s", [key_hash, ts, count,])
            
            for row in xrange(cursor.rowcount):
                tasks.append(self.fetchoneDict(cursor))
            
            cursor.close()
            return tasks
        
        return None
        
    def addJobs(self, values):
        cursor = self.db._cursor()
        cursor.executemany("INSERT INTO `"+re.escape(self.tube_name)+"` (task,ttr,ts) VALUES (%s, %s, %s)", values)
        stuff = {'count' : cursor.rowcount, 'id':cursor.lastrowid}
        cursor.close()
        return stuff
        #return {'count':self.db.executemany_rowcount("INSERT INTO `"+re.escape(self.tube_name)+"` (task,ttr,ts) VALUES (%s, %s, %s)", values),'id':cursor.lastrowid}
    
        
    def peekJobs(self, limit=1, ts=time.time()):
        cursor = self.db._cursor()
        cursor.execute("SELECT task,ts,jobId FROM `"+re.escape(self.tube_name)+"` WHERE (state=0 and ts < %s) OR (state=1 and ts+ttr < %s) ORDER BY jobId ASC LIMIT %s", [ts, ts, limit])
        
        if cursor.rowcount > 0:
            tasks = list()
            for row in xrange(cursor.rowcount):
                tasks.append(self.fetchoneDict(cursor))
                
            cursor.close()
            return tasks
        
        return None #return none when we got no rows back
    
    def changeState(self, state=0, key_hash=None, ts=None, Ids=None):
        sql = self.id_key_limit(key_hash=key_hash, Ids=Ids)        
        new_ts = time.time()# ts that will be used with the update
         
        cursor = self.db._cursor()
        cursor.execute("UPDATE `"+re.escape(self.tube_name)+"` SET ts=%s, state=%s WHERE 1=1 "+sql['Ids']+" "+sql['key_hash']+" "+sql['limit'], [new_ts,state])
        count = cursor.rowcount
        cursor.close()
        
        return count
        
    def updateClock(self, key_hash=None, ts=None, Ids=None):
        sql = self.id_key_limit(key_hash=key_hash, Ids=Ids)   
        new_ts = time.time()# ts that will be used with the update
         
        cursor = self.db._cursor()
        cursor.execute("UPDATE `"+re.escape(self.tube_name)+"` SET ts=%s WHERE 1=1 "+sql['Ids']+" "+sql['key_hash']+" "+sql['limit'], [new_ts])
        count = cursor.rowcount
        cursor.close()
        
        return {'count' : count, 'ts' : new_ts}
        
    def delJobs(self, key_hash=None, ts=None, Ids=None):
        sql = self.id_key_limit(key_hash=key_hash, Ids=Ids) 
         
        cursor = self.db._cursor()
        cursor.execute("DELETE FROM `"+re.escape(self.tube_name)+"` WHERE 1=1 "+sql['Ids']+" "+sql['key_hash']+" "+sql['limit'])
        count = cursor.rowcount
        cursor.close()
        
        return count
    
    """
    Helpers are below
    """
    def fetchoneDict(self, cursor):
        row = cursor.fetchone()
        if row is None: return None
        cols = [ d[0] for d in cursor.description ]
        return dict(zip(cols, row))
        
    def randomKey(self):
        return hashlib.sha1(str(random.randint(0, 999999999999999999))+self.request.remote_ip+str(random.randint(0, 999999999999999999))).hexdigest()
    
    def id_key_limit(self, key_hash=None, Ids=None): #felt like i was doing this way to many times
        if Ids != None:
            limit = "LIMIT "+str(len(Ids))
            Ids = "AND jobID IN (%s)" % ",".join(str(x) for x in Ids)
        else:
            limit, Ids = "", ""
            
        if key_hash != None:
            use_key_hash = "AND key_hash=\""+re.escape(key_hash)+"\""
        else:
            use_key_hash = ""
        return {'key_hash': use_key_hash, 'Ids': Ids, 'limit': limit} #return the vaules
    
    def clean_id_list(self):
        id_list = list()
        if self.payload.has_key('Ids'):
            for Id in payload['Ids']:
                if type(Id) is int:
                    id_list.append(Id)
        
        if len(id_list) == 0:
            return None
        
        return id_list
        
class jobGet(tornado.web.RequestHandler, queueDB):
    """
    This will need to be able to get from more then one queue and be able to get more then one item
    """
    def get(self, queue, job=None):
        self.setQueue(queue)
        self.set_header("Content-Type", "application/json")
        hash_key = self.randomKey()
        
        jobs = self.claimJobs(hash_key, ttr=int(self.get_argument("ttr",60)), limit=int(self.get_argument("limit",1)), ts=float(self.get_argument("ts",time.time())))
        if jobs != None:
            self.write(json_encode({'tube' : queue, 'jobs': jobs, 'hash_key' : hash_key}))
        else:
            self.write(json_encode({'tube' : queue, 'job': None, 'id':None, 'error' : 'No job avaiable'}))

class jobAdd(tornado.web.RequestHandler, queueDB):
    """
    Make sure that we at some point add a comma check so it will write the same data to each tube
    We will also need to apply the same change to getting.
    Needs to support multi write.
    """
    def put(self, queue):
        self.set_header("Content-Type", "application/json")
        
        try:
            self.setQueue(queue)
            value = list()
            for job in json_decode(self.request.body)['jobs']:
                if job.has_key('delay') == False:
                    job['delay'] = 0
                    
                if job.has_key('time') == False:
                    job['time'] = time.time()
                    
                if job.has_key('ttr') == False:
                    job['ttr'] = 0
                
                value.append([job['job'], job['ttr'], job['time']+job['delay']])
            
            job_id = self.addJobs(value)
            if job_id['count'] > 0:
                self.write(json_encode({'tube' : queue, 'id': job_id['id'], 'count':job_id['count']})) #not returning the right number of added documents
            else:
                self.write(json_encode({'tube' : queue, 'id': False}))
                
        except tornado.web.MissingArgumentError:
            self.write(json_encode({'tube' : queue, 'error': 'No job provided'}))
        
        self.db.close()

class jobPeek(tornado.web.RequestHandler, queueDB): #just look at the jobs in the system but dont do anything with them
    def get(self, queue, Id=None):
        self.setQueue(queue)
        self.set_header("Content-Type", "application/json")
        job = self.peekJobs(limit=int(self.get_argument("limit",1)), ts=float(self.get_argument("ts",time.time())))
        if job != None:
            self.write(json_encode({'tube' : queue, 'jobs': job}))
        else:
            self.write(json_encode({'tube' : queue, 'jobs': None, 'error' : 'No job avaiable'}))

class jobTouch(tornado.web.RequestHandler, queueDB): #touch a job to update its time    
    def post(self, queue):
        self.setQueue(queue)
        self.set_header("Content-Type", "application/json")
        key_hash, ts = self.get_argument("key_hash", None), self.get_argument("ts", None) #get some values from the query string
        if ts is int: ts = float(ts) #if we got the vaule ts lets make sure that its no longer a string but a float
        self.payload = json_decode(self.request.body)
        id_list = self.clean_id_list() #get paylayload and decode, get a list of ids from body
        
        if key_hash != None or id_list != None:
            if id_list != None or ts != None: #if we dont have ids we need to use key_hash with a time
                touch = self.updateClock(key_hash=key_hash, ts=ts, Ids=id_list)
                if touch > 0:
                    self.write(json_encode({'tube' : queue, 'ts': touch['ts'], 'count':touch['count']}))
                else:
                    self.write(json_encode({'tube' : queue, 'ts': touch['ts'], 'count': False}))
            else:
                self.write(json_encode({'tube' : queue, 'error': 'Trying to use key_hash but missing ts'}))
        else:#be cause we had no key hash or ids
            self.write(json_encode({'tube' : queue, 'error': 'Missing key_hash or Ids to work with'}))
        
    
class jobDel(tornado.web.RequestHandler, queueDB):
    def delete(self, queue):
        self.setQueue(queue)
        self.set_header("Content-Type", "application/json")
        key_hash, ts = self.get_argument("key_hash", None), self.get_argument("ts", None) #get some values from the query string
        if ts is int: ts = float(ts) #if we got the vaule ts lets make sure that its no longer a string but a float
        self.payload = json_decode(self.request.body)
        id_list = self.clean_id_list() #get paylayload and decode, get a list of ids from body
        
        if key_hash != None or id_list != None:
            if id_list != None or ts != None: #if we dont have ids we need to use key_hash with a time
                delete = self.delJobs(key_hash=key_hash, ts=ts, Ids=id_list)
                if touch > 0:
                    self.write(json_encode({'tube' : queue, 'count':delete}))
                else:
                    self.write(json_encode({'tube' : queue, 'count': False}))
            else:
                self.write(json_encode({'tube' : queue, 'error': 'Trying to use key_hash but missing ts'}))
        else:#be cause we had no key hash or ids
            self.write(json_encode({'tube' : queue, 'error': 'Missing key_hash or Ids to work with'}))
        
class jobFree(tornado.web.RequestHandler, queueDB): #touch a job to update its time    
    def post(self, queue):
        self.setQueue(queue)
        self.set_header("Content-Type", "application/json")
        key_hash, ts = self.get_argument("key_hash", None), self.get_argument("ts", None) #get some values from the query string
        if ts is int: ts = float(ts) #if we got the vaule ts lets make sure that its no longer a string but a float
        self.payload = json_decode(self.request.body)
        id_list = self.clean_id_list() #get paylayload and decode, get a list of ids from body
        
        if key_hash != None or id_list != None:
            if id_list != None or ts != None: #if we dont have ids we need to use key_hash with a time
                free = self.changeState(state=0, key_hash=key_hash, ts=ts, Ids=id_list)
                if touch > 0:
                    self.write(json_encode({'tube' : queue, 'count':free}))
                else:
                    self.write(json_encode({'tube' : queue, 'count': False}))
            else:
                self.write(json_encode({'tube' : queue, 'error': 'Trying to use key_hash but missing ts'}))
        else:#be cause we had no key hash or ids
            self.write(json_encode({'tube' : queue, 'error': 'Missing key_hash or Ids to work with'}))

class jobBury(tornado.web.RequestHandler, queueDB):
    def post(self, queue):
        self.setQueue(queue)
        self.set_header("Content-Type", "application/json")
        key_hash, ts = self.get_argument("key_hash", None), self.get_argument("ts", None) #get some values from the query string
        if ts is int: ts = float(ts) #if we got the vaule ts lets make sure that its no longer a string but a float
        self.payload json_decode(self.request.body)
        id_list = self.clean_id_list() #get paylayload and decode, get a list of ids from body
        
        if key_hash != None or id_list != None:
            if id_list != None or ts != None: #if we dont have ids we need to use key_hash with a time
                bury = self.changeState(state=-1, key_hash=key_hash, ts=ts, Ids=id_list)
                if touch > 0:
                    self.write(json_encode({'tube' : queue, 'count':bury}))
                else:
                    self.write(json_encode({'tube' : queue, 'count': False}))
            else:
                self.write(json_encode({'tube' : queue, 'error': 'Trying to use key_hash but missing ts'}))
        else:#be cause we had no key hash or ids
            self.write(json_encode({'tube' : queue, 'error': 'Missing key_hash or Ids to work with'}))
            
    
        
application = tornado.web.Application([
    (r"/queue/(.*)/job/get", jobGet), #get the next job
    (r"/queue/(.*)/job/add", jobAdd),
    (r"/queue/(.*)/job/touch", jobTouch),
    (r"/queue/(.*)/job/delete", jobDel),
    (r"/queue/(.*)/job/free", jobFree),
    (r"/queue/(.*)/job/peek", jobPeek),
    (r"/queue/(.*)/job/bury", jobBury),
    #(r"/queue/(.*)/job/kick", MainHandler),
])

if __name__ == "__main__":
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()
