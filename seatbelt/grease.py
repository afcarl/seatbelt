# os-independent filesystem -> db script

import couchdb
import time
import os
from Queue import Queue
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


def valid_filename(x):
    return not ('~' in x or x.startswith('.') or x.startswith('#'))

def get_ddocname(path):
    return "_design/%s" % (os.path.basename(os.path.normpath(path)))

def sync(ddoc_dir, db_uri):
    db = couchdb.Database(db_uri)

    ddocname = get_ddocname(ddoc_dir)
    if not ddocname in db:
        db[ddocname] = {"_id": ddocname}

    # Initialize
    for fname in os.listdir(ddoc_dir):
        if valid_filename(fname):
            ddoc = db[ddocname]
            db.put_attachment(ddoc, open(os.path.join(ddoc_dir, fname)))

class Ev2Q(FileSystemEventHandler):
    def __init__(self, ev_queue):
        self.ev_queue = ev_queue
        FileSystemEventHandler.__init__(self)

    def on_created(self, ev):
        self.ev_queue.put(["created", ev])
    def on_deleted(self, ev):
        self.ev_queue.put(["deleted", ev])
    def on_modified(self, ev):
        self.ev_queue.put(["modified", ev])

def watch(ddoc_dir, db_uri):
    ddocname = get_ddocname(ddoc_dir)
    db = couchdb.Database(db_uri)
    
    obs = Observer()
    q = Queue()
    e2q = Ev2Q(q)
    obs.schedule(e2q, ddoc_dir, recursive=False)

    try:
        obs.start()
        while True:
            # XXX: Keyboard interrupts won't stop this blocking call.
            # We could add a timeout to catch this in a reasonable amount of time.
            evtype, ev = q.get()
            name = ev.src_path
            fullpath = os.path.join(ddoc_dir, name)
            if evtype in ["created", "modified"] and valid_filename(name) and not os.path.isdir(fullpath):
                print evtype, fullpath
                ddoc = db[ddocname]
                db.put_attachment(ddoc, open(fullpath))
    finally:
        obs.stop()
    obs.join()

if __name__=='__main__':
    import sys
    watchdir = sys.argv[1]
    uri = sys.argv[2]
    sync(watchdir, uri)
    watch(watchdir, uri)
