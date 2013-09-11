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
        self.db.execute("CREATE TABLE IF NOT EXISTS tube (ID INTEGER PRIMARY KEY AUTOINCREMENT,value TEXT NOT NULL,state INTEGER NOT NULL,ts INTEGER,ttr INTERGER)")
        
    def tubeExists(self):
        c = self.db.cursor()
        c.execute("SELECT COUNT(*) as count FROM sqlite_master WHERE type='table' AND name='tube' LIMIT 1");
        if c.fetchone()['count'] == 1:
            return True
        else:
            return False
    
    def tubeAddTask(self, task):
        c = self.db.cursor()
        c.execute("INSERT INTO tube (value,state,ts) VALUES (?, 0, ?);", (task, time.time(), ))
        self.db.commit()
        if c.rowcount > 0:
            return c.lastrowid
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
        
class taskGet(tornado.web.RequestHandler):
    def get(self, queue, task=None):
        self.write(queue)
        #self.write(task)
        self.write("Hello, world")

class taskAdd(tornado.web.RequestHandler, queueDB):
    def put(self, queue):
        try:
            self.setQueue(queue)
            
            if self.tubeExists() == False:
                self.tubeCreate()
            
            task_id = self.tubeAddTask(self.get_argument('task'))
            if task_id != False:
                self.write(json_encode({'tube' : queue, 'id': task_id}))
            else:
                self.write(json_encode({'tube' : queue, 'id': False}))
                
        except tornado.web.MissingArgumentError:
            self.write(json_encode({'tube' : queue, 'error': 'No task provided'}))
        
        self.db.close()

application = tornado.web.Application([
    (r"/queue/(.*)/task/get", taskGet), #get the next task
    (r"/queue/(.*)/task/get/(.*)", taskGet), #get an exact task
    (r"/queue/(.*)/task/add", taskAdd),
    (r"/task/touch", MainHandler),
    (r"/task/rm", MainHandler),
    (r"/task/peak", MainHandler),
    (r"/task/free", MainHandler),
    (r"/task/bury", MainHandler),
    (r"/task/kick", MainHandler),
])

if __name__ == "__main__":
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()