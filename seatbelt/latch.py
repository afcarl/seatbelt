from seatbelt import serve

import couchdb
import json
import multiprocessing
import time
import traceback
import types
import urllib
import urlparse

def log(*a):
    print "LOG", a
    #print "/".join([(X) for X in a])

class ConMan(object):
    def __init__(self, server_uri, stooge_dbname="conman"):
        self.server_uri = server_uri
        self.server = couchdb.Server(server_uri)

        if stooge_dbname not in self.server:
            self.stooge_db = self.server.create(stooge_dbname)
        else:
            self.stooge_db = self.server[stooge_dbname]

        self.meta_doc = self.stooge_db.get("meta", {
            "_id": "meta",
            "dbs": {}
        })

        self.init()
        self.ensure_therapist_ddocs()
        self.update_all_dbs()

    def init(self):
        pass

    def ensure_therapist_ddocs(self):
        pass

    def update_all_dbs(self):
        pass

    def run_forever(self):
        # XXX: duplicated code from psychotherapist -- abstract
        db_changes_url = urlparse.urljoin(self.server_uri, "_db_updates")
        for raw in urllib.urlopen(db_changes_url):
            if len(raw.strip()) > 0:
                res = json.loads(raw)
                change_type = res["type"]
                db_name = res["db_name"]
                self.db_dispatch(change_type, db_name)

    def db_dispatch(self, change_type, db_name):
        # As with doc_dispatch, returning True causes event to *not* bubble
        if self._dispatch("db_%s_%s" % (db_name, change_type), db_name):
            return
        if self._dispatch("db_%s" % (change_type), db_name):
            return

    def _dispatch(self, name, *a, **kw):
        if hasattr(self, name):
            return getattr(self, name)(*a, **kw)

    def db_updated(self, db_name):
        log("db_updated", db_name)
        db = self.server[db_name]
        self.handle_changes(db)

    def handle_changes(self, db):
        old_update_seq = self.meta_doc["dbs"].setdefault(db.name, {}).setdefault("update_seq", 0)
        new_update_seq = db.info()["update_seq"]
        if old_update_seq == new_update_seq:
            log("no change?", db.name, old_update_seq)
            return

        onlymeta = True
        for change in db.changes(since=old_update_seq, include_docs=True)["results"]:
            if db.name == self.stooge_db.name and change["doc"].get("type") == "job":
                # run job

                # TODO: This would be a place to enable multiple
                # job-nodes by using the job `status' field as a mutex.
                if change["doc"]["status"] == "new":
                    doc = change["doc"]
                    doc_db = self.server[doc["db"]]
                    change_type = doc["change_type"]
                    changed_doc = doc["doc"]

                    if change_type == "updated":
                        latest_doc = doc_db[changed_doc["_id"]]
                        if latest_doc.get("_rev") != changed_doc.get("_rev"):
                            # Don't run job if doc has been updated again.
                            doc["status"] = "skipped"
                            #self.stooge_db.save(doc)
                            continue

                    doc["status"] = "success"
                    try:
                        self.doc_dispatch(change_type, doc_db, changed_doc)
                        if change_type == "created":
                            # Also dispatch to `updated'
                            self.doc_dispatch("updated", doc_db, changed_doc)
                    except:
                        # Error running job -- log
                        log("error running job", changed_doc["_id"], "job-id", doc["_id"])
                        doc["status"] = "failed"
                        doc["error"] = traceback.format_exc()
                    doc["rtime"] = time.time()
                    self.stooge_db.save(doc)

                onlymeta = False
                continue

            # Don't dispatch on the meta-doc (infinite loop!)
            if db.name == self.stooge_db.name and change["id"] == "meta":
                continue
            onlymeta = False

            if change.get("deleted"):
                change_type = "deleted"
                if change["id"] in self.meta_doc["dbs"][db.name]["ids"]:
                    del self.meta_doc["dbs"][db.name]["ids"][change["id"]]
                else:
                    continue
            elif change["id"] in self.meta_doc["dbs"][db.name].setdefault("ids", {}):
                change_type = "updated"
                self.meta_doc["dbs"][db.name]["ids"][change["id"]] = change["seq"]
            else:
                change_type = "created"
                self.meta_doc["dbs"][db.name]["ids"][change["id"]] = change["seq"]

            # create a job doc
            job_doc = {"change_type": change_type, 
                       "doc": change["doc"], 
                       "db": db.name,
                       "ctime": time.time(),
                       "type": "job",
                       "status": "new"}
            self.stooge_db.save(job_doc)

            self.meta_doc["dbs"][db.name]["update_seq"] = change["seq"]

        if not onlymeta and old_update_seq != new_update_seq:
            self.meta_doc["dbs"][db.name]["update_seq"] = new_update_seq
            self.stooge_db.save(self.meta_doc)

    def doc_dispatch(self, change_type, db, doc):
        a = [db, doc]
        # From most- to least-specific.
        # Returning True causes event to *not* bubble
        doctype = doc.get("type", "").replace("-","_")
        if self._dispatch("db_%s_doc_%s_id_%s" % (db.name, change_type, doc["_id"]), *a):
            return
        if self._dispatch("db_%s_doc_%s_type_%s" % (db.name, change_type, doctype), *a):
            return
        if self._dispatch("db_%s_doc_%s" % (db.name, change_type), *a):
            return
        if self._dispatch("doc_%s_id_%s" % (change_type, doc["_id"]), *a):
            return
        if self._dispatch("doc_%s_type_%s" % (change_type, doctype), *a):
            return
        if self._dispatch("doc_%s" % (change_type), *a):
            return

class ConDuckt(ConMan):
    # extensible ConMan
    def ensure_therapist_ddocs(self):
        ConMan.ensure_therapist_ddocs(self)

        for docid in self.stooge_db:
            doc = self.stooge_db[docid]
            if doc.get("type") == "code" and doc.get("status") != "error":
                self._attach_code(doc["_id"], doc["code"])

    def _attach_code(self, name, code, args=None):
        # munge raw code into a function
        if args is None:
            args = ["self", "db", "doc"]
            if name == "init":
                args = ["self"]

        code = "def %s(%s):\n%s\n" % (
            name, ', '.join(args), 
            '\n'.join(['    %s' % (x) for x in code.split('\n')]))

        code_obj = compile(code, name, 'exec')
        ns = {}
        exec code_obj in ns
        setattr(self, name, types.MethodType(ns[name], self))
        log("attached code", name)

        # Run `init' if it's been updated -- let's hope it's idempotent!
        if name == "init":
            self.init()

    def db_conman_doc_updated_type_code(self, db, doc):
        if doc.get("status") == "error":
            return

        code = doc["code"]
        name = doc["_id"]

        try:
            self._attach_code(name, code)
        except:
            doc["status"] = "error"
            doc["error"] = traceback.format_exc()
            db.save(doc)

    def db_conman_doc_deleted_type_code(self, db, doc):
        log("TODO", "db_conman_doc_deleted_type_code")

def run(dbdir, conman=ConDuckt, **kw):
    q = multiprocessing.Queue()
    kw["queue"] = q
    p = multiprocessing.Process(target=serve, args=(dbdir,), kwargs=kw)
    p.start()

    # Block on Queue until the server has started
    root_uri = q.get()

    c = conman(root_uri)
    print root_uri.replace("/root/", "")
    c.run_forever()

if __name__=='__main__':
    import sys
    run(sys.argv[1])
