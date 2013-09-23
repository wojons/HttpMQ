import tornado.ioloop
import tornado.web
from tornado.escape import json_encode
import tornado.database as database

import time

class queueDB():
    def stateChangeByKey():
        UPDATE ? SET key=?, state=?, ts=? WHERE key=? and ts=? and state=? LIMIT ?
        
    def stateChangeById():
        UPDATE ? SET key=?, state=?, ts=? WHERE jobID IN (?) key=? and ts=? and state=? LIMIT ?
        
    def claimJobs(self, key, limit=1 ttr=60, ts=None):
        if ts 
        UPDATE ? SET key=? state=1, ts=UNIX_TIMESTAMP(), ttr=? WHERE (state=0 and ts < ) OR (state=1 and ts+ttr < ?) ORDER BY jobId ASC LIMIT ?
        SELECT * FROM ? WHERE key=? AND ts=? ORDERBY jobId ASC LIMIT ?
        
    def addJobs():
        INSERT INTO ? (job,ttr,ts) VALUES (?, ?, ?)
        
    def peekJobs():
        SELECT * FROM ? WHERE (state=0 and ts < ?) OR (state=1 and ts+ttr < ?) ORDER BY jobId ASC LIMIT ?
        
    def deleteJobByKey():
        DELETE FROM ? WHERE key=? and ts=? and state=? LIMIT ?
    
    def deleteJobById()
        DELETE FROM ? WHERE jobId IN (?) and key LIMIT ?
    

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")
        
class taskGet(tornado.web.RequestHandler, queueDB):
    """
    This will need to be able to get from more then one queue and be able to get more then one item
    """
    def get(self, queue, task=None):
        self.setQueue(queue)
        task = self.tubeGetTask(ttr=self.get_argument("ttr",None), limit=self.get_argument("limit",1))
        if task != None:
            self.write(json_encode({'tube' : queue, 'task': task['task'], 'id':task['id'], 'ts':task['ts']}))
        else:
            self.write(json_encode({'tube' : queue, 'task': None, 'id':None, 'error' : 'No task avaiable'}))

class taskAdd(tornado.web.RequestHandler, queueDB):
    """
    Make sure that we at some point add a comma check so it will write the same data to each tube
    We will also need to apply the same change to getting.
    Needs to support multi write.
    """
    def put(self, queue):
        try:
            self.setQueue(queue)
            
            if self.tubeExists() == False:
                self.tubeCreate()
                
            task_id = self.tubeAddTask(self.get_argument('task'), ttr=self.get_argument("ttr",60), ts=self.get_argument("ts",time.time()))
            if task_id != False:
                self.write(json_encode({'tube' : queue, 'id': task_id}))
            else:
                self.write(json_encode({'tube' : queue, 'id': False}))
                
        except tornado.web.MissingArgumentError:
            self.write(json_encode({'tube' : queue, 'error': 'No task provided'}))
        
        self.db.close()

class taskPeek(tornado.web.RequestHandler, queueDB): #just look at the tasks in the system but dont do anything with them
    def get(self, queue, Id=None):
        self.setQueue(queue)
        task = self.tubePeekTask(Id)
        if task != None:
            self.write(json_encode({'tube' : queue, 'task': task['task'], 'id':task['id']}))
        else:
            self.write(json_encode({'tube' : queue, 'task': None, 'id':None, 'error' : 'No task avaiable'}))

class taskTouch(tornado.web.RequestHandler, queueDB): #touch a task to update its time
    def get(self, queue, Id):
        self.setQueue(queue)
        touch = self.tubeTouchTask(Id, self.get_argument("ts",time.time()))
        if touch != False:
            self.write(json_encode({'tube' : queue, 'id': Id, 'ts':touch}))
        else:
            self.write(json_encode({'tube' : queue, 'id': Id, 'ts': False}))
        
    def delete(self, queue, Id):
class taskRm(tornado.web.RequestHandler, queueDB):
        self.setQueue(queue)
        rm = self.tubeRmTask(Id, self.get_argument("ts",None))
        self.db.close()
        self.write(json_encode({'tube' : queue, 'id': Id, 'deleted':rm}))
        
class taskFree(tornado.web.RequestHandler, queueDB):
    def get(self, queue, Id):
        self.setQueue(queue)
        free = self.tubeFreeTask(Id, self.get_argument("ts",None))
        self.db.close()
        self.write(json_encode({'tube' : queue, 'id': Id, 'freed':free}))

class taskBury(tornado.web.RequestHandler, queueDB):
    def get(self, queue, Id, ts=None):
        self.setQueue(queue)
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