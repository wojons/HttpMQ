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
    """
    def stateChangeByKey():
        UPDATE ? SET key=?, state=?, ts=? WHERE key=? and ts=? and state=? LIMIT ?
        
    def stateChangeById():
        UPDATE ? SET key=?, state=?, ts=? WHERE jobID IN (?) key=? and ts=? and state=? LIMIT ?
   """     
    def claimJobs(self, key_hash, limit=1, ttr=60, ts=time.time()):
        cursor = self.db._cursor()
        cursor.execute("UPDATE `"+re.escape(self.tube_name)+"` SET key_hash=%s, state=1, ts=%s, ttr=%s WHERE (state=0 and ts < %s) OR (state=1 and ts+ttr < %s) ORDER BY jobId ASC LIMIT %s", [key_hash, ts, ttr, ts, ts, limit,])
        count = cursor.rowcount
        cursor.close()
        if count > 0:
            tasks = list()
            cursor = self.db._cursor()
            cursor.execute("SELECT task,ts,jobId,ttr FROM `"+re.escape(self.tube_name)+"` WHERE key_hash=%s AND ts=%s ORDER BY jobId ASC LIMIT %s", [key_hash, ts, count,])
            
            for row in xrange(cursor.rowcount):
                tasks.append(self.fetchoneDict(cursor))
            
            cursor.close()
            return tasks
        
        else:
            return None
        
    def addJobs(self, values):
        cursor = self.db._cursor()
        cursor.executemany("INSERT INTO `"+re.escape(self.tube_name)+"` (task,ttr,ts) VALUES (%s, %s, %s)", values)
        stuff = {'count' : cursor.rowcount, 'id':cursor.lastrowid}
        cursor.close()
        return stuff
        #return {'count':self.db.executemany_rowcount("INSERT INTO `"+re.escape(self.tube_name)+"` (task,ttr,ts) VALUES (%s, %s, %s)", values),'id':cursor.lastrowid}
        
    def fetchoneDict(self, cursor):
        row = cursor.fetchone()
        if row is None: return None
        cols = [ d[0] for d in cursor.description ]
        return dict(zip(cols, row))
        
    def randomKey(self):
        return hashlib.sha1(str(random.randint(0, 999999999999999999))+self.request.remote_ip+str(random.randint(0, 999999999999999999))).hexdigest()
    
    """    
    def peekJobs():
        SELECT * FROM ? WHERE (state=0 and ts < ?) OR (state=1 and ts+ttr < ?) ORDER BY jobId ASC LIMIT ?
        
    def deleteJobByKey():
        DELETE FROM ? WHERE key=? and ts=? and state=? LIMIT ?
    
    def deleteJobById()
        DELETE FROM ? WHERE jobId IN (?) and key LIMIT ?
    """

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")
        
class taskGet(tornado.web.RequestHandler, queueDB):
    """
    This will need to be able to get from more then one queue and be able to get more then one item
    """
    def get(self, queue, task=None):
        self.setQueue(queue)
        self.set_header("Content-Type", "application/json")
        hash_key = self.randomKey()
        
        tasks = self.claimJobs(hash_key, ttr=int(self.get_argument("ttr",60)), limit=int(self.get_argument("limit",1)))
        if tasks != None:
            self.write(json_encode({'tube' : queue, 'tasks': tasks, 'hash_key' : hash_key}))
        else:
            self.write(json_encode({'tube' : queue, 'task': None, 'id':None, 'error' : 'No task avaiable'}))

class taskAdd(tornado.web.RequestHandler, queueDB):
    """
    Make sure that we at some point add a comma check so it will write the same data to each tube
    We will also need to apply the same change to getting.
    Needs to support multi write.
    """
    def put(self, queue):
        self.set_header("Content-Type", "application/json")
        
        try:
            self.setQueue(queue)
            print self.request.body
            value = list()
            for message in json_decode(self.request.body)['messages']:
                if message.has_key('delay') == False:
                    message['delay'] = 0
                    
                if message.has_key('time') == False:
                    message['time'] = time.time()
                    
                if message.has_key('ttr') == False:
                    message['ttr'] = 0
                
                value.append([message['task'], message['ttr'], message['time']+message['delay']])
            
            task_id = self.addJobs(value)
            if task_id['count'] > 0:
                self.write(json_encode({'tube' : queue, 'id': task_id['id'], 'count':task_id['count']})) #not returning the right number of added documents
            else:
                self.write(json_encode({'tube' : queue, 'id': False}))
                
        except tornado.web.MissingArgumentError:
            self.write(json_encode({'tube' : queue, 'error': 'No task provided'}))
        
        self.db.close()

class taskPeek(tornado.web.RequestHandler, queueDB): #just look at the tasks in the system but dont do anything with them
    def get(self, queue, Id=None):
        self.setQueue(queue)
        self.set_header("Content-Type", "application/json")
        
        task = self.tubePeekTask(Id)
        if task != None:
            self.write(json_encode({'tube' : queue, 'task': task['task'], 'id':task['id']}))
        else:
            self.write(json_encode({'tube' : queue, 'task': None, 'id':None, 'error' : 'No task avaiable'}))

class taskTouch(tornado.web.RequestHandler, queueDB): #touch a task to update its time    
    def get(self, queue, Id):
        self.setQueue(queue)
        self.set_header("Content-Type", "application/json")
        
        touch = self.tubeTouchTask(Id, self.get_argument("ts",time.time()))
        if touch != False:
            self.write(json_encode({'tube' : queue, 'id': Id, 'ts':touch}))
        else:
            self.write(json_encode({'tube' : queue, 'id': Id, 'ts': False}))
        
    
class taskRm(tornado.web.RequestHandler, queueDB):
    def delete(self, queue, Id):
        self.setQueue(queue)
        self.set_header("Content-Type", "application/json")
        
        rm = self.tubeRmTask(Id, self.get_argument("ts",None))
        self.db.close()
        self.write(json_encode({'tube' : queue, 'id': Id, 'deleted':rm}))
        
class taskFree(tornado.web.RequestHandler, queueDB):
    def get(self, queue, Id):
        self.setQueue(queue)
        self.set_header("Content-Type", "application/json")
        
        free = self.tubeFreeTask(Id, self.get_argument("ts",None))
        self.db.close()
        self.write(json_encode({'tube' : queue, 'id': Id, 'freed':free}))

class taskBury(tornado.web.RequestHandler, queueDB):
    def get(self, queue, Id, ts=None):
        self.setQueue(queue)
        self.set_header("Content-Type", "application/json")
        
        free = self.tubeBuryTask(Id, self.get_argument("ts",time.time()))
        self.db.close()
        self.write(json_encode({'tube' : queue, 'id': Id, 'bury':free}))
        
application = tornado.web.Application([
    (r"/queue/(.*)/task/get", taskGet), #get the next task
    (r"/queue/(.*)/task/get/(.*)", taskGet), #get an exact task
    (r"/queue/(.*)/task/add", taskAdd),
    (r"/queue/(.*)/task/touch/(.*)", taskTouch),
    (r"/queue/(.*)/task/rm/(.*)", taskRm),
    (r"/queue/(.*)/task/free/(.*)", taskFree),
    (r"/queue/(.*)/task/peek", taskPeek),
    (r"/queue/(.*)/task/peek/(.*)", taskPeek),
    (r"/queue/(.*)/task/bury/(.*)", MainHandler),
    (r"/queue/(.*)/task/kick", MainHandler),
])

if __name__ == "__main__":
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()
