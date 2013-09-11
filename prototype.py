import tornado.ioloop
import tornado.web
from tornado.escape import json_encode

import sqlite3
import time

class queueDB():        
    def setQueue(self, queue):
        self.db = sqlite3.connect('/tmp/'+queue+'.db')
        self.db.row_factory = self.dict_factory
        
    def tubeCreate(self, force=None):
        self.db.execute("CREATE TABLE IF NOT EXISTS tube (ID INTEGER PRIMARY KEY AUTOINCREMENT,task TEXT NOT NULL,state INTEGER NOT NULL,ts DOUBLE,ttr INTERGER)")
        
    def tubeExists(self):
        c = self.db.cursor()
        c.execute("SELECT COUNT(*) as count FROM sqlite_master WHERE type='table' AND name='tube' LIMIT 1");
        if c.fetchone()['count'] == 1:
            return True
        else:
            return False
    
    def tubeAddTask(self, task, ttr=60):
        c = self.db.cursor()
        c.execute("INSERT INTO tube (task,state,ts,ttr) VALUES (?, 0, ?, ?);", (task, time.time(), ttr,))
        self.db.commit()
        if c.rowcount > 0:
            return c.lastrowid
        else:
            return False
    def tubeGetTask(self, Id=None, ttr=None):
        c = self.db.cursor()
        c.execute("BEGIN EXCLUSIVE TRANSACTION");
        c.execute("SELECT ID as id,task FROM tube WHERE state=0 OR (state=1 AND ts+ttr < ? ) ORDER BY ID ASC LIMIT 1", (time.time(),))
        task = c.fetchone()
        if task != None:
            ts = time.time();
            if ttr == None:
                c.execute("UPDATE tube SET state=1,ts=? WHERE ID=?", (ts, task['id']))
            else:
                c.execute("UPDATE tube SET state=1,ts=?,ttr=? WHERE ID=?", (ts,ttr, task['id']))
            task['ts'] = ts
        else:
            task = None
        
        self.db.commit()
        return task
    def tubeTouchTask(self, Id, timestamp):
        """
        update the timestamp of an open task
        if a user has taken a task and they need more time before ttr
        they should use this to buy more time
        """
        c = self.db.cursor()
        ts = time.time()
        c.execute("UPDATE tube SET ts=? WHERE ID=? AND state=1 AND ts=?", (ts,Id,timestamp,))
        self.db.commit()
        if c.rowcount > 0:
            return ts
        else:
            return False
            
    def tubeFreeTask(self, Id, timestamp=None):
        """
        update a task and changes its state to 0
        if a timestamp is provded it uses it to match
        """
        c = self.db.cursor()
        ts = time.time()
        if timestamp != None:
            c.execute("UPDATE tube SET ts=?,state=0 WHERE ID=? AND state=1 AND ts=?", (ts,Id,timestamp,))
            print "UPDATE tube SET ts=%s,state=0 WHERE ID=%s AND state=1 AND ts=%s" % (ts,Id,timestamp,)
        else:
            c.execute("UPDATE tube SET ts=?,state=0 WHERE ID=? AND state=1", (ts,Id,))
            
        self.db.commit()
        if c.rowcount > 0:
            return ts
        else:
            return False
            
    def tubeBuryTask(self, Id, timestamp=None):
        """
        update a task and changes its state to -1
        if a timestamp is provded it uses it to match
        """
        c = self.db.cursor()
        ts = time.time()
        if timestamp != None:
            c.execute("UPDATE tube SET ts=?,state=-1 WHERE ID=? AND state=1 AND ts=?", (ts,Id,timestamp,))
        else:
            c.execute("UPDATE tube SET ts=?,state=-1 WHERE ID=? AND state=1", (ts,Id,))
            
        self.db.commit()
        if c.rowcount > 0:
            return ts
        else:
            return False
    
    def tubeRmTask(self, Id, ts=None):
        """
        Delete a task using its Id, if the timestamp
        is provded it will check with that
        """
        c = self.db.cursor()
        if ts==None:
            c.execute("DELETE FROM tube WHERE ID=?", (Id,))
        else:
            c.execute("DELETE FROM tube WHERE ID=? AND ts=?", (Id,ts,))
        self.db.commit()
        
        if c.rowcount > 0:
            return True
        else:
            return False
        
    def dict_factory(self, cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")
        
class taskGet(tornado.web.RequestHandler, queueDB):
    def get(self, queue, task=None):
        self.setQueue(queue)
        ttr = self.get_argument("ttr",None)
        task = self.tubeGetTask(ttr=ttr)
        if task != None:
            self.write(json_encode({'tube' : queue, 'task': task['task'], 'id':task['id'], 'ts':task['ts']}))
        else:
            self.write(json_encode({'tube' : queue, 'task': None, 'id':None, 'error' : 'No task avaiable'}))

class taskAdd(tornado.web.RequestHandler, queueDB):
    def put(self, queue):
        try:
            self.setQueue(queue)
            
            if self.tubeExists() == False:
                self.tubeCreate()
            ttr = self.get_argument("ttr",60)
            task_id = self.tubeAddTask(self.get_argument('task'), ttr=ttr)
            if task_id != False:
                self.write(json_encode({'tube' : queue, 'id': task_id}))
            else:
                self.write(json_encode({'tube' : queue, 'id': False}))
                
        except tornado.web.MissingArgumentError:
            self.write(json_encode({'tube' : queue, 'error': 'No task provided'}))
        
        self.db.close()

class taskTouch(tornado.web.RequestHandler, queueDB):
    def get(self, queue, Id):
        self.setQueue(queue)
        touch = self.tubeTouchTask(Id, self.get_argument("ts",time.time()))
        if touch != False:
            self.write(json_encode({'tube' : queue, 'id': Id, 'ts':touch}))
        else:
            self.write(json_encode({'tube' : queue, 'id': Id, 'ts': False}))
        
class taskRm(tornado.web.RequestHandler, queueDB):
    def delete(self, queue, Id):
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
        
application = tornado.web.Application([
    (r"/queue/(.*)/task/get", taskGet), #get the next task
    (r"/queue/(.*)/task/get/(.*)", taskGet), #get an exact task
    (r"/queue/(.*)/task/add", taskAdd),
    (r"/queue/(.*)/task/touch/(.*)", taskTouch),
    (r"/queue/(.*)/task/rm/(.*)", taskRm),
    (r"/queue/(.*)/task/free/(.*)", taskFree),
    (r"/queue/(.*)/task/peak", MainHandler),
    (r"/queue/(.*)/task/bury/(.*)", MainHandler),
    (r"/queue/(.*)/task/kick", MainHandler),
])

if __name__ == "__main__":
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()
