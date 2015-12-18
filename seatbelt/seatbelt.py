# twisted, portable, incomplete fs-backed couchdb-inspired database

"""
python seatbelt.py /path/to/database_directory

curl http://localhost:6984/_all_dbs

curl -X PUT http://localhost:6984/db1

curl http://localhost:6984/db1/_all_docs

curl -X PUT -d '{"foo": "bar"}' http://localhost:6984/db1/foo

curl http://localhost:6984/db1/_all_docs

curl -X PUT --data-binary @FILE_PATH http://localhost:6984/db1/foo/FILE_NAME

curl http://localhost:6984/db1/foo/FILE_NAME

curl http://localhost:6984/db1/_all_docs

curl http://localhost:6984/db1/_changes

curl -X DELETE http://localhost:6984/db1/foo

curl -X PUT -d '{}' http://localhost:6984/db1/_design/testing

curl -X PUT --data-binary @FILE_PATH http://localhost:6984/db1/_design/testing/index.html

curl http://localhost:6984/db1/_design/testing/index.html

curl http://localhost:6984/db1/_design/testing/_rewrite/

"""

import codecs
import gzip
import hashlib
import json
import mimetypes
import os
import shutil
import tempfile
import time
import urllib
import uuid
from cStringIO import StringIO


from threading import Thread, Lock, Event
from Queue import Queue, Empty

from autobahn.twisted.websocket import WebSocketServerProtocol, \
                                       WebSocketServerFactory
from autobahn.twisted.resource import WebSocketResource

from twisted.web.static import File
from twisted.web.resource import Resource
from twisted.web.server import Site, NOT_DONE_YET
from twisted.internet import reactor

def valid_filename(x):
    return not ('~' in x or x.startswith('.') or x.startswith('#'))

def get_ddocname(path):
    return "_design/%s" % (os.path.basename(os.path.normpath(path)))


def make_id():
    return uuid.uuid4().get_hex()
def make_rev(doc):
    revno = int(doc.get("_rev", "0-").split("-")[0])
    return "%d-%d" % (revno + 1, time.time())
def valid_id(s):
    return len(s.strip()) > 0 and (not (s.startswith("_") or '/' in s)) or id_is_ddoc(s)
def id_is_ddoc(s):
    return s.startswith("_design/") and len(s) > 8 and (not "." in s) and s.find("/",8) < 0
def make_change(doc, seq_no):
    return {"doc": doc, "id": doc["_id"], "deleted": doc.get("_deleted", False), "seq": seq_no}
def make_all_docs(docmap):
    return {"rows": [{"doc": X, "id": X["_id"]} for X in docmap.values() if not X.get("_deleted")]}
def json_dumps(obj):
    return json.dumps(obj, ensure_ascii=True, encoding='utf-8')
def json_dumpsu(obj):
    return json.dumps(obj, ensure_ascii=True, encoding='utf-8').encode("utf-8")
def json_dump(obj, fp):
    return json.dump(obj, fp, ensure_ascii=False)
def openw(path):
    return codecs.open(path, mode='w', encoding='utf8')
def opena(path):
    return codecs.open(path, mode='a', encoding='utf8')

ALLOW_CROSS_ORIGIN = True

def _cors(request):
    if ALLOW_CROSS_ORIGIN:
        request.setHeader('Access-Control-Allow-Methods', 'GET, PUT, POST, DELETE, OPTIONS')
        request.setHeader('Access-Control-Allow-Origin', '*')
        request.setHeader('Access-Control-Allow-Headers', 'Content-Type')

# Store all of the "parts" of a databse in a dictionary so that one
# part can be changed and, so long as the function signature matches,
# things should "just work."
#
# This implies reasonable encapsulation of internals, which is somwhat
# wishful thinking.

class PartsBin:
    def __init__(self):
        self._parts = {}

    def __getitem__(self, key):
        if key in self._parts:
            return self._parts[key]
        elif key in globals():
            return globals()[key]
        raise KeyError("Cannot find part: %s" % (key))
    def __setitem__(self, key, val):
        self._parts[key] = val

PARTS_BIN = PartsBin()

class SynchronousFileSink():
    def __init__(self, filepath):
        self.filepath = filepath

    def put(self, msg):
        with opena(self.filepath) as fh:
            fh.write("%s\n" % (msg))

    def stop(self):
        pass

class AsynchronousFileSink(Thread):
    """The AsynchronousFileSink is a thread that dumps lines of text to a
    file, while allowing synchronous reads of that file
    (ie. guaranteeing that nothing has been added while a read is in
    progress).
    """
    def __init__(self, filepath):
        self.filepath = filepath
        # XXX: will this handle all the unicode data correctly?
        # I am using open() instead of codecs.open() because
        # I also want it to work with binary data.
        # TODO: make isBinary an initialization flag to AFS.
        self.fh = open(self.filepath, 'a+', 1) # line-buffered (?)
        self.msg_queue = Queue()
        self.msg_lock = Lock()
        self.quit_event = Event()
        Thread.__init__(self, target=self._run_forever)
        self.start()

    def _run_forever(self):
        while True:
            if self.quit_event.is_set():
                self.fh.close()
                return
            try:
                line, isBinary = self.msg_queue.get(timeout=0.1)
            except Empty:
                continue
            with self.msg_lock:
                if isBinary:
                    self.fh.write(line)
                else:
                    # Add a newline character if the message is not binary
                    self.fh.write("%s\n" % (line))

    def read(self):
        with self.msg_lock:
            self.fh.close()     # sync file to disk
            out = open(self.filepath).read()
            self.fh = opena(self.filepath)
        return out

    def put(self, msg, isBinary=False):
        "thread-safe message append function"
        self.msg_queue.put([msg, isBinary])

    def stop(self):
        self.quit_event.set()
        self.join()

class GetJSON(Resource):
    def __init__(self, doc):
        Resource.__init__(self)
        self.doc = doc
    def render_GET(self, request):
        _cors(request)

        request.headers["Content-Type"] = "application/json"
        return json_dumpsu(self.doc)


class GetAllJSON(Resource):
    # Lazy (cached) JSON representation of _all_docs
    def __init__(self, doc):
        Resource.__init__(self)
        self.doc = doc
        self._cache = None
    def wipe_cache(self):
        self._cache = None
    def render_GET(self, request):
        _cors(request)

        if self._cache is None:
            self._cache = make_all_docs(self.doc)
        request.headers["Content-Type"] = "application/json"
        return json_dumpsu(self._cache)


class DbUpdates(Resource):
    def __init__(self):
        Resource.__init__(self)
        self._change_waiters = {} # request -> timeout
    def render_GET(self, request):
        _cors(request)

        # XXX: only implemented as feed=continuous&timeout=60
        self._change_waiters[request] = reactor.callLater(
            60, self._change_timeout, request)
        request.notifyFinish().addErrback(
            self._change_nevermind, request)
        return NOT_DONE_YET
    def _change_timeout(self, request):
        # Write an empty newline and reset timeout
        request.write("\n")
        self._change_waiters[request] = reactor.callLater(
            60, self._change_timeout, request)
    def _change_nevermind(self, _err, request):
        self._change_waiters[request].cancel()
        del self._change_waiters[request]
    def _change(self, change_type, db_name):
        # Notify all of the _change_waiters
        for req,timeout in self._change_waiters.items():
            req.write("%s\n" % (json_dumpsu({"type": change_type, "db_name": db_name})))
            # reset timeout
            timeout.cancel()
            self._change_waiters[req] = reactor.callLater(60, self._change_timeout, req)


class DbChangesWsFactory(WebSocketServerFactory):
    def __init__(self, db, url=None):
        self.db = db
        WebSocketServerFactory.__init__(self, url)
        self.clients = {}       # peerstr -> client
        self.binmeta = {}       # peerstr -> upcoming attachment info

    def register(self, client):
        self.clients[client.peer] = client

        # Add client document to database
        self.db._try_update({"_id": client.peer,
                             "type": "peer",
                             "name": client.peer}, initiator=client)
        
        # Send _all_docs to client
        client.sendMessage(json.dumps(self.db._all_docs))
        
        # Send a special `whoami' document
        client.sendMessage(json.dumps({"type": "whoami", "name": client.peer}))

    def unregister(self, client):
        if client.peer in self.clients and client.peer in self.db.docs:
            #print "unregistered client", client.peer
            del self.clients[client.peer]

            # Delete client doc
            self.db.delete_doc(client.peer)
            
        else:
            print "??? unregistering an unregistered client", client.peer

    def stop(self):
        # Disconnect all clients
        peerids = self.clients.keys()
        self.clients = {}       # no more updates...
        
        for peer in peerids:
            if peer in self.db.docs:
                # Delete client doc
                self.db.delete_doc(peer)

    def _send(self, msg, initiator=None):
        for c in self.clients.values():
            if c != initiator:
                c.sendMessage(msg)

class DbChangesWsProtocol(WebSocketServerProtocol):
    def onOpen(self):
        self.factory.register(self)

    def connectionLost(self, reason):
        WebSocketServerProtocol.connectionLost(self, reason)
        self.factory.unregister(self)

    def onMessage(self, payload, isBinary):
        if not isBinary:
            doc = json.loads(payload)
            # If type is "_attach", treat as attachment metadata
            if doc.get("type") == "_attach":
                self.factory.clients[self.peer] = doc
            else:
                # Interpret incoming comands as database updates
                if doc.get("_deleted", False):
                    did_delete = self.factory.db.delete_doc(doc["_id"], initiator=self)
                    upd = {"ok": did_delete, "_id": doc["_id"]}
                else:
                    upd = self.factory.db._try_update(doc, initiator=self)

                # Indicate success
                # TODO: indicate failure
                self.sendMessage(json.dumps(upd))
        else:
            metadoc = self.factory.clients[self.peer]
            del self.factory.clients[self.peer]
            docid = metadoc["id"]
            attachname = metadoc["name"]
            dbdoc = self.factory.db.docs.get(docid)

            reactor.callInThread(dbdoc._async_put_attachment, StringIO(payload), None, attachname)
            
class CorsWebSocketResource(WebSocketResource):
    def getChild(self, name, request):
        _cors(request)
        return WebSocketResource.getChild(self, name, request)


class DbChanges(Resource):
    def __init__(self, db):
        Resource.__init__(self)
        self.db = db
        self._change_waiters = {} # request -> timeout

        # Create a websocket child
        self._change_sockets = PARTS_BIN["DbChangesWsFactory"](self.db)
        self._change_sockets.protocol = PARTS_BIN["DbChangesWsProtocol"]
        self._change_sockets_resource = CorsWebSocketResource(self._change_sockets)
        self.putChild("_ws", self._change_sockets_resource)

    def stop(self):
        self._change_sockets.stop()

    def render_GET(self, request):
        _cors(request)

        request.headers["Content-Type"] = "application/json"
        if request.args.get("since") is not None:
            since = int(request.args["since"][0])
            # Return immediately if `since' is in the past, otherwise wait for next change
            if since < self.db._db_info["update_seq"]:
                updates = [make_change(self.db._changes[K], K) for K in sorted([X for X in self.db._changes.keys() if X>=since])]
                return json_dumpsu({"results": updates, "last_seq": self.db._db_info["update_seq"]+1})

        call = reactor.callLater(
            60, self._change_timeout, request)
        self._change_waiters[request] = call
        request.notifyFinish().addErrback(
            self._change_nevermind, request)
        return NOT_DONE_YET

    def _change_timeout(self, request):
        # Write an empty newline and reset timeout
        request.write("\n")
        self._change_waiters[request] = reactor.callLater(
            60, self._change_timeout, request)
    def _change_nevermind(self, _err, request):
        self._change_waiters[request].cancel()
        del self._change_waiters[request]
    def _change(self, doc, initiator=None):
        msg = json_dumpsu({
            "results": [make_change(doc, self.db._db_info["update_seq"])],
            "last_seq": self.db._db_info["update_seq"]+1
        })

        # Notify all of the _change_waiters
        self._change_sockets._send(msg, initiator=initiator)
        for req,timeout in self._change_waiters.items():
            req.write(msg)
            req.finish()
            timeout.cancel()
            del self._change_waiters[req]

class Attachment(File):
    def __init__(self, kid, path, **kw):
        self.kid = kid
        File.__init__(self, path, **kw)
    def render_PUT(self, request):
        _cors(request)
        return self.kid.render_PUT(request)

    def getChild(self, name, request):
        _cors(request)
        return File.getChild(self, name, request)

class Stream(Resource):
    def __init__(self, filepath):
        # Serve the file, with bonus features...
        Resource.__init__(self)
        self.filepath = filepath
        self.stream_factory = StreamFactory(filepath)
        self.stream_factory.protocol = StreamProtocol
        self.stream_factory._stream_res = self
        self.stream_resource = CorsWebSocketResource(self.stream_factory)
        self.putChild("_ws", self.stream_resource)
        self.putChild("meta", StreamMeta(self))

    def render_GET(self, req):
        _cors(req)
        # XXX: content type?
        return self.stream_factory.sink.read()

    def getChild(self, name, req):
        if name == '':
            return self
            #return File(self.filepath)
        return Resource.getChild(self, name, req)

    def stop(self):
        self.stream_factory.sink.stop()

class StreamMeta(Resource):
    def __init__(self, stream):
        self.stream = stream
        Resource.__init__(self)
    def render_GET(self, req):
        req.headers["Content-Type"] = "application/json"
        return json_dumpsu({
            "path": self.stream.filepath,
            "clients": self.stream.stream_factory.clients.keys()
            })

class StreamFactory(WebSocketServerFactory):
    def __init__(self, sinkpath):
        WebSocketServerFactory.__init__(self, None)

        self.clients = {}       # peerstr -> client
        # self.sink = AsynchronousFileSink(sinkpath)

    @property
    def do_record(self):
        return self._stream_res.do_record

class StreamProtocol(WebSocketServerProtocol):
    def onOpen(self):
        print 'connected stream client', self.peer, self.factory.clients.keys()
        self.factory.clients[self.peer] = self
    def connectionLost(self, reason):
        if self.peer in self.factory.clients:
            print 'disconnected stream client', self.peer
            del self.factory.clients[self.peer]
    def onMessage(self, payload, isBinary):
        # print 'sending payload to %d connected clients' % (len(self.factory.clients)-1)
        for client in self.factory.clients.values():
            # Send to all clients excepting self
            if client.peer != self.peer:
                # print 'sending message from', self.peer, 'to', client.peer, '; len: ', len(payload)
                client.sendMessage(payload, isBinary=isBinary)

        # print 'got message!'
        if self.factory.do_record:
            #print 'saving message!', payload
            self.factory.sink.put(payload, isBinary)

class Document(Resource):
    def __init__(self, db, docpath):
        Resource.__init__(self)
        self.db = db
        self.docpath = docpath
        self._docid = os.path.basename(docpath)
        self.attachments = {}    # name -> File
        self.streams = {}        # name -> Stream
        for name in self.doc.get("_streams", {}):
            self._serve_stream(name)
        self._load_from_disk()

    @property
    def docid(self):
        return self._docid

    @property
    def doc(self):
        return self.db.getdoc(self.docid)

    def _load_from_disk(self):
        if os.path.exists(self.docpath):
            for filename in os.listdir(self.docpath):
                if filename in self.doc.get("_attachments", {}):
                    self._serve_attachment(filename, self._get_mime(filename))

    def _serve_stream(self, streamname):
        # websocket-based streaming
        if streamname not in self.streams:
            # Make sure a directory for this document exists
            if not os.path.isdir(self.docpath):
                os.makedirs(self.docpath)
            self.streams[streamname] = Stream(os.path.join(self.docpath, streamname))
            self.putChild(streamname, self.streams[streamname])
        # Set recording flag
        self.streams[streamname].do_record = self.doc["_streams"][streamname].get("recording", False)

    def _serve_attachment(self, filename, mimetype="text/html"):
        self.attachments[filename] = PARTS_BIN["Attachment"](self, os.path.join(self.docpath, filename),
                                              defaultType=mimetype)
        self.putChild(filename, self.attachments[filename])

    def render_GET(self, request):
        _cors(request)

        request.headers["Content-Type"] = "application/json"
        return json_dumpsu(self.db.getdoc(self.docid))

    def link_attachment(self, path, filename=None):
        # for programmatic use only
        # (Only works on systems that support symlinks)

        if not os.path.exists(self.docpath):
            os.makedirs(self.docpath)

        if filename is None:
            filename = os.path.basename(path)
        else:
            # XXX: Should be more consistent about "security"
            filename = os.path.basename(filename)
        a_dest = os.path.join(self.docpath, filename)
        os.symlink(os.path.abspath(path), a_dest)
        self._create_attachment(filename)

    def stop(self):
        for s in self.streams.values():
            s.stop()

    def put_attachment(self, req, filename=None):
        fh = req.content

        attachname = filename or fh.name

        reactor.callInThread(self._async_put_attachment, fh, req, attachname)

    def _async_put_attachment(self, fh, req, attachname):

        # Write to a temporary location, while computing the sha1
        sha1 = hashlib.sha1()
        filesize = 0

        # Write file to temporary location on disk and compute size/hash
        # TODO: Make hash computation optional
        with tempfile.NamedTemporaryFile(delete=False) as fp:
            for chunk in fh:
                sha1.update(chunk)
                fp.write(chunk)
                filesize += len(chunk)

        # Save to _attachments/
        hashstr = sha1.hexdigest()
        apath = self._get_attachpath(hashstr)
        if not os.path.exists(apath):
            dname = os.path.dirname(apath)
            if not os.path.exists(dname):
                os.makedirs(dname)
            shutil.move(fp.name, apath)
            os.chmod(apath, 0444) # Treat attachments as immutable
        else:
            os.remove(fp.name)

        a_dest = os.path.join(self.docpath, attachname)
        if os.path.exists(a_dest):
            os.chmod(a_dest, 0777) # ...may be needed to remove "immutable" file
            os.remove(a_dest)

        if not os.path.exists(self.docpath):
            os.makedirs(self.docpath)

        # Create symlink to payload
        if hasattr(os, "symlink"):
            os.symlink(
                os.path.relpath(apath, start=self.docpath), # Use relative links in case the db dir moves
                a_dest)
        else:
            # On platforms without symlinks (*ahem* Windows),
            # simply move the payload
            os.rename(apath, a_dest)

        reactor.callFromThread(self._create_attachment, attachname, filesize=filesize, hashstr=hashstr, req=req)

    def _create_attachment(self, attachname, filesize=None, hashstr=None, req=None):
        content_type = self._get_mime(attachname)
        if hashstr is None:
            hashstr = ""
        if filesize is None:
            # Calculate filesize
            filesize = os.path.getsize(os.path.realpath(os.path.join(self.docpath, attachname)))
        self.doc.setdefault("_attachments", {})[attachname] = {
            "stub": True, 
            "digest": hashstr,
            "length": filesize,
            "content_type": content_type}
        self._serve_attachment(attachname, mimetype=content_type)
        update = self.db._try_update(self.doc)
        if req is not None:
            req.write(json_dumpsu(update))
            req.finish()
        else:
            return update

    def render_PUT(self, request):
        _cors(request)

        #path = request.path.replace("/_design%2F", "/_design/")
        path = urllib.unquote(request.path)
        rempath = path[path.index(self.docid)+len(self.docid)+1:].strip()

        if len(rempath) > 0:
            # attachment
            request.headers["Content-Type"] = "application/json"

            # Don't bother verifying Revision ID! Mua-ha-ha!
            # revid = request.args["rev"][0]
            # if revid != self.doc["_rev"]:
            #     return json_dumpsu({"error": "revid mismatch"})

            self.put_attachment(request, filename=rempath)
            return NOT_DONE_YET

        else:
            # trying to update document -- handle in parent
            return self.db.render_PUT(request)

    def _get_mime(self, name):
        return mimetypes.guess_type(name)[0]

    def _get_attachpath(self, hashstr):
        # Where, based on the hashstr, should the attachment be found?
        return os.path.join(
            self.db.seatbelt.datadir,
            "_attachments",
            hashstr[:2],
            hashstr[2:])

    def render_DELETE(self, request):
        _cors(request)

        request.headers["Content-Type"] = "application/json"
        if self.db.delete_doc(self.docid):
            return json_dumpsu({"ok": True})
        else:
            return json_dumpsu({"error": "not found or revid mismatch"})

    def render_OPTIONS(self, request):
        # XXX: This is needed for DELETE to work.
        _cors(request)
        return ""

    def getChild(self, name, request):
        _cors(request)

        if request.method in ["PUT", "OPTIONS"] and len(name) > 0:
            return self
        return Resource.getChild(self, name, request)

class Designer(Resource):
    def __init__(self, db):
        self.db = db
        Resource.__init__(self)
    def getChild(self, name, request):
        _cors(request)

        if request.method == "PUT" and len(name) > 0:
            return self.db.getChild(name, request)
        return Resource.getChild(self, name, request)

class DesignDoc(Document):
    def __init__(self, *a, **kw):
        Document.__init__(self, *a, **kw)

        # Pretend that we have rewrites set up such that this
        # documents' attachments are visible, along with the parent
        # database.
        # Assumes that this document has (or will have) an index.html
        # attachment, to use as the _rewrite/ root.
        # TODO: implement _rewrite/ semantics
        self.rewrite_resource = File(self.docpath)
        self.rewrite_resource.indexNames = ["index.html"]
        self.rewrite_resource.putChild("db", self.db)
        self.rewrite_resource.putChild("root", self.db.seatbelt) # insecure!

        # Include seatbelt.js
        self.rewrite_resource.putChild("seatbelt.js", File(os.path.join(
            os.path.dirname(__file__), 'static', 'seatbelt.js')))

        self.putChild("_rewrite", self.rewrite_resource)

        # Create symlink structure so that static version will work
        if hasattr(os, "symlink"):
            if not os.path.exists(self.docpath):
                os.makedirs(self.docpath)
            rpath = os.path.join(self.docpath, "_rewrite")
            if not os.path.exists(rpath):
                # Since we don't actually support rewrites, the _rewrite URL points right back here
                os.symlink(".", rpath)
            dpath = os.path.join(self.docpath, "db")
            if not os.path.exists(dpath):
                os.symlink(
                    os.path.relpath(self.db.dbpath, start=self.docpath),
                    dpath)
            rpath = os.path.join(self.docpath, "root")
            if not os.path.exists(rpath):
                os.symlink(
                    os.path.relpath(self.db.seatbelt.datadir, start=self.docpath),
                    rpath)

    @property
    def docid(self):
        return "_design/%s" % (self._docid)

    def render_GET(self, request):
        _cors(request)

        request.headers["Content-Type"] = "application/json"
        return json_dumpsu(self.db.getdoc(self.docid))

class Database(Resource):
    def __init__(self, seatbelt, dbpath):
        Resource.__init__(self)
        self.seatbelt = seatbelt
        self._all_docs = {}
        self.dbpath = dbpath
        self.dbname = os.path.basename(dbpath)

        self.change_resource = PARTS_BIN["DbChanges"](self)
        self.putChild("_changes", self.change_resource)

        # defaults -- potentially overwritten in `self._load_from_disk()' call
        self.docs = {}          # docid -> Document
        self._db_info = {"db_name": self.dbname, "update_seq": 0}
        self._changes = {}      # seqno -> [doc]

        self._load_from_disk()
        self.all_docs_resource = GetAllJSON(self._all_docs)

        # We need to use an intermediate object for the Designer
        # because _design/<name> is interpreted by twisted as two
        # levels deep.
        self.designer_resource = PARTS_BIN["Designer"](self)
        self.putChild("_design", self.designer_resource)

        self._serve_docs()

        self.putChild("_all_docs", self.all_docs_resource)

        #self._changesink = AsynchronousFileSink(os.path.join(self.dbpath, "_changes"))
        self._changesink = SynchronousFileSink(os.path.join(self.dbpath, "_changes"))        

        self._change_waiters = {} # request -> timeout_callback

    def stop(self):
        self.change_resource.stop()
        
        self._changesink.stop()
        
        for doc in self.docs.values():
            doc.stop()

    def _save_to_disk(self):
        if not os.path.exists(self.dbpath):
            os.makedirs(self.dbpath)
        self._save_all_docs()
        self._save_db_info()

    def _save_db_info(self):
        db_info_file = os.path.join(self.dbpath, "_db_info")
        json_dump(self._db_info, openw(db_info_file))

    def _save_all_docs(self):
        all_docs_tmp = os.path.join(self.dbpath, "_all_docs.tmp")
        all_docs_old = os.path.join(self.dbpath, "_all_docs.old")

        json_dump(make_all_docs(self._all_docs), openw(all_docs_tmp))
        all_docs_file = os.path.join(self.dbpath, "_all_docs")

        # os.rename doesn't clobber on all platforms
        if os.path.exists(all_docs_file):
            os.rename(all_docs_file, all_docs_old)

        os.rename(all_docs_tmp, all_docs_file)

        if os.path.exists(all_docs_old):
            os.unlink(all_docs_old)

        # Wipe _changes -- they should be incorporated in _all_docs by now
        changes_file = os.path.join(self.dbpath, "_changes")
        openw(changes_file).write('')

    def _load_from_disk(self):
        all_docs_file = os.path.join(self.dbpath, "_all_docs")
        if os.path.exists(all_docs_file):
            rows = json.load(open(all_docs_file))
            for row in rows["rows"]:
                self._all_docs[row["doc"]["_id"]] = row["doc"]

        # Incorporate _changes
        changes_file = os.path.join(self.dbpath, "_changes")
        if os.path.exists(changes_file):
            for line in open(changes_file):
                if len(line.strip()) > 2:
                    c = json.loads(line)
                    self._all_docs[c["id"]] = c["doc"]

            # Archive _changes file as _changes.%d.gz
            changes_fh = open(changes_file)
            changes_out_pattern = os.path.join(self.dbpath, "_changes.%d.gz")
            changes_out_idx = 1
            while os.path.exists(changes_out_pattern % (changes_out_idx)):
                changes_out_idx += 1
            changes_out_fh = gzip.open(changes_out_pattern % (changes_out_idx), 'wb')
            changes_out_fh.writelines(changes_fh)
            changes_out_fh.close()
            changes_fh.close()

        db_info_file = os.path.join(self.dbpath, "_db_info")
        if os.path.exists(db_info_file):
            self._db_info = json.load(open(db_info_file))

        self._save_to_disk()

    def _serve_docs(self):
        for docid in self._all_docs:
            self._serve_doc(docid)
    def _serve_doc(self, docid):
        # only create resource if doc is new
        if docid not in self.docs:
            if id_is_ddoc(docid):
                self.docs[docid] = PARTS_BIN["DesignDoc"](self, os.path.join(self.dbpath, docid))
                self.designer_resource.putChild(docid.split("/")[1], self.docs[docid])

                # Also put as a child here -- for some reason HEAD requests stop here otherwise?!
                # ie. checking if _design/foo is in the DB from python-couchdb
                self.putChild(docid, self.docs[docid])

            else:
                self.docs[docid] = PARTS_BIN["Document"](self, os.path.join(self.dbpath, docid))
                self.putChild(docid, self.docs[docid])

    def getdoc(self, docid):
        return self._all_docs[docid]

    def create_doc(self, doc):
        upd = self._try_update(doc)
        if upd.get("ok"):
            return self.docs[doc["_id"]]

    def delete_doc(self, docid, revid=None,initiator=None):
        doc = self.getdoc(docid)
        docobj = self.docs[docid]

        if True: #doc.get("_volatile") or doc.get("_rev") == revid:
            del self._all_docs[docid]
            del self.docs[docid]
            #self.db._save_to_disk()
            self._save_db_info()
            
            self._change({"_id": docid, "_deleted": True}, initiator=initiator)

            self.all_docs_resource.wipe_cache()

            # remove attachments
            for attachname in docobj.attachments:
                os.unlink(os.path.join(docobj.docpath, attachname))
            # remove streams (XXX: archive?)
            for streamname in docobj.streams:
                docobj.streams[streamname].stop()
                os.unlink(os.path.join(docobj.docpath, streamname))
            if os.path.exists(docobj.docpath):
                os.rmdir(docobj.docpath)

            return True
        return False

    def _try_update(self, doc, initiator=None):
        docid = doc["_id"]
        if not valid_id(docid):
            return {"error": "invalid id"}

        # if docid in self._all_docs and self._all_docs[docid].get("_rev") != doc.get("_rev"):
        #     return {"error": "revision conflict"}

        # don't increment `rev' on `_volatile' updates
        if not doc.get("_volatile"):
            doc["_rev"] = make_rev(doc)

        self._all_docs[docid] = doc
        self.all_docs_resource.wipe_cache()
        self._serve_doc(docid)

        # Initiate new streams
        for name in doc.get("_streams", {}):
            self.docs[docid]._serve_stream(name)

        # Send document to anyone watching DB changes
        self._change(doc, initiator=initiator)

        # update _db_info
        if not doc.get("_volatile"):
            self._save_db_info()

        return {"ok": True, "rev": doc.get("_rev"), "id": doc["_id"]}

    def render_GET(self, request):
        _cors(request)

        request.headers["Content-Type"] = "application/json"
        return json_dumpsu(self._db_info)

    def _change(self, doc, initiator=None):
        # Propagate to carousel
        self.seatbelt._change("updated", self.dbname)

        # Serialize update
        if not doc.get("_volatile"):
            self._changes[self._db_info["update_seq"]] = doc

            # Sync change to disk
            self._changesink.put(json_dumps(make_change(doc, self._db_info["update_seq"])))
            # with opena(os.path.join(self.dbpath, "_changes")) as fh:
            #     fh.write("%s\n" % (json_dumps(make_change(doc, self._db_info["update_seq"]))))

        self.change_resource._change(doc, initiator=initiator)

        if not doc.get("_volatile"):
            self._db_info["update_seq"] += 1
        
    def _change_timeout(self, request):
        del self._change_waiters[request]
        request.write(json_dumpsu({}))
        request.finish()
    def _change_nevermind(self, _err, request):
        self._change_waiters[request].cancel()
        del self._change_waiters[request]

    def render_PUT(self, request):
        _cors(request)

        doc = json.load(request.content)

        docid = request.path.split("/")[-1]
        if request.path.split("/")[-2] == "_design":
            docid = "_design/%s" % (docid)
        else:
            # Un-escape the docid
            docid = urllib.unquote(docid)
            # In case the / of a ddoc has been escaped (what's our escape policy more generally?)
            #docid = docid.replace("%2F", "/")

        doc["_id"] = docid

        request.headers["Content-Type"] = "application/json"
        return json_dumpsu(self._try_update(doc))

    def render_POST(self, request):
        _cors(request)

        doc = json.load(request.content)
        if doc.get("_id") is None:
            doc["_id"] = make_id()

        request.headers["Content-Type"] = "application/json"
        return json_dumpsu(self._try_update(doc))

    def getChild(self, name, request):
        _cors(request)

        if (request.method == "PUT" and len(name) > 0) or (request.method in ["GET", "POST"] and len(name) == 0):
            return self
        return Resource.getChild(self, name, request)

class Seatbelt(Resource):
    def __init__(self, datadir):
        Resource.__init__(self)
        self.datadir = datadir

        if not os.path.exists(datadir):
            os.makedirs(datadir)

        self.db_updates_resource = PARTS_BIN["DbUpdates"]()
        self.putChild("_db_updates", self.db_updates_resource)

        self._all_dbs = []
        self.dbs = {}           # dbname -> Database
        self._load_from_disk()
        self._serve_dbs()

        self.all_dbs_resource = GetJSON(self._all_dbs)
        self.putChild("_all_dbs", self.all_dbs_resource)

    def stop(self):
        # Stop all databases
        for db in self.dbs.values():
            db.stop()

    def _change(self, change_type, db_name):
        self.db_updates_resource._change(change_type, db_name)

    def render_GET(self, request):
        _cors(request)

        request.headers["Content-Type"] = "application/json"        
        return(json_dumpsu({"db": "seatbelt", "version": -1}))

    def get_or_create_db(self, name):
        if name in self.dbs:
            return self.dbs[name]
        return self.create_db(name)

    def create_db(self, name):
        if not valid_id(name):
            raise RuntimeError
        self._all_dbs.append(name)
        self._save_to_disk()
        self._serve_db(name)

        self._change("created", name)
        return self.dbs[name]

    def render_PUT(self, request):
        _cors(request)

        name = request.path.split("/")[-1]
        self.create_db(name)

        request.headers["Content-Type"] = "application/json"
        return json_dumpsu({"ok": True})

    def render_DELETE(self, request):
        # XXX: TODO
        pass

    def _load_from_disk(self):
        all_dbs_file = os.path.join(self.datadir, "_all_dbs")
        if not os.path.exists(all_dbs_file):
            self._save_to_disk()

        self._all_dbs = json.load(open(all_dbs_file))

    def _save_to_disk(self):
        all_dbs_file = os.path.join(self.datadir, "_all_dbs")
        json_dump(self._all_dbs, open(all_dbs_file, 'w'))

    def _serve_dbs(self):
        for dbname in self._all_dbs:
            self._serve_db(dbname)

    def _serve_db(self, dbname):
        self.dbs[dbname] = PARTS_BIN["Database"](self, os.path.join(self.datadir, dbname))
        self.putChild(dbname, self.dbs[dbname])

    def getChild(self, name, req):
        _cors(req)

        if req.method=='PUT' and len(name) > 0:
            # db creation
            return self
        elif req.method=='GET' and len(name) == 0:
            # db info (XXX: why do I need this to be explicit?)
            # Shouldn't the render_GET be called by default?
            return self
        return Resource.getChild(self, name, req)

class PirateBelt(Seatbelt):
    # Piratepad-style Seatbelt variant

    # make /links go to the _rewrite url of a database that's created
    # & populated on-demand
    def __init__(self, dbdir, landing_path, ddoc_path, otherddocs=None):
        self.ddoc_path = ddoc_path
        self.otherddocs = otherddocs or []
        self.ddoc_name = "_design/%s" % (ddoc_path.split("/")[-1])
        self.file_resource = File(landing_path)
        Seatbelt.__init__(self, dbdir)

    def _serve_db(self, dbname):
        # Add the _rewrite root, instead of the db root
        self.dbs[dbname] = PARTS_BIN["Database"](self, os.path.join(self.datadir, dbname))
        # Link the ddocs
        ddoc = linkddocs(self.dbs[dbname], self.ddoc_path)
        self.putChild(dbname, ddoc.rewrite_resource)

        for odd in self.otherddocs:
            o_ddoc = linkddocs(self.dbs[dbname], odd)
            ddoc.rewrite_resource.putChild(odd.split("/")[-1], o_ddoc.rewrite_resource)

    def on_db_create(self, db):
        pass

    def getChild(self, name, request):
        _cors(request)

        if request.method == "GET":
            # Let's assume that all file are either empty (-> index.html) or have a period in them.
            if len(name) == 0 or "." in name:
                return self.file_resource.getChild(name, request)
            elif "_" in name:
                # Why *not* allow some of the db tracking APIs...
                return self
            else:
                # get_or_create db?
                db = self.create_db(name)

                self.on_db_create(db)

                # Return new ddoc _rewrite
                return db.docs[self.ddoc_name].rewrite_resource
        else:
            # Error? 
            return Resource.getChild(self, name, request)

def _getddoc(db, srcdir):
    ddocname = get_ddocname(srcdir)

    if ddocname in db.docs:
        ddoc = db.docs[ddocname]
    else:
        ddoc = db.create_doc({"_id": ddocname, "type": "design"})

    return ddoc

def linkddocs(db, srcdir, copy=False):
    ddoc = _getddoc(db, srcdir)
    for fname in os.listdir(srcdir):
        if valid_filename(fname) and fname not in ddoc.doc.get("_attachments", {}):
            ddoc.link_attachment(os.path.join(srcdir, fname))

    return ddoc

def trackddocs(db, srcdir, db_uri):
    import grease
    import multiprocessing
    
    ddoc = _getddoc(db, srcdir)

    def _track():
        # XXX: Ideally, we would `sync' synchronously and block until
        # completion, but this seems simpler to implement.
        grease.sync(srcdir, db_uri)
        grease.watch(srcdir, db_uri)
    
    return ddoc, multiprocessing.Process(target=_track)

def _add_endpoints(resource, endpoints):
    for k,v in endpoints.items():
        resource.putChild(k, v)

def serve(dbdir, port=6984, interface='0.0.0.0', queue=None, defaultdb=None, defaultddocs=None, ddoclink=False, otherddocs=[], otherdbs=[], endpoints={}):
    seatbelt = PARTS_BIN["Seatbelt"](dbdir)
    site = Site(seatbelt)

    local_root_uri = "http://%s:%d" % (interface, port)
    do_track = []
    if defaultdb is not None:
        db = seatbelt.get_or_create_db(defaultdb)
        if defaultddocs is not None:
            if ddoclink:
                ddoc = linkddocs(db, defaultddocs)
            else:
                ddoc, _track = trackddocs(db, defaultddocs, local_root_uri + "/db")
                do_track.append(_track)

            for odd in otherddocs:
                if ddoclink:
                    o_ddoc = linkddocs(db, odd)
                else:
                    o_ddoc, _track = trackddocs(db, odd, local_root_uri + "/db")
                    do_track.append(_track)
                ddoc.rewrite_resource.putChild(odd.split("/")[-1], o_ddoc.rewrite_resource)

            site = Site(ddoc.rewrite_resource)
            local_root_uri += "/root/"
            _add_endpoints(ddoc.rewrite_resource, endpoints)
        else:
            _add_endpoints(seatbelt, endpoints)    

        # Create other databases, as requested
        # XXX: Sometime soon, the URL scheme should be revised & firmly diverged from CouchDB.
        for dbname in otherdbs:
            seatbelt.get_or_create_db(dbname)

    reactor.listenTCP(port, site, interface=interface)

    for t in do_track:
        t.start()

    if queue is not None:
        queue.put(local_root_uri)

    reactor.addSystemEventTrigger('before', 'shutdown', seatbelt.stop)

    reactor.run()

if __name__=='__main__':
    import sys
    if len(sys.argv) == 2:
        serve(sys.argv[1])
    elif len(sys.argv) == 1:
        import tempfile
        tdir = tempfile.mkdtemp()
        print "Serializing to: %s" % (tdir)
        serve(tdir)
    else:
        print "usage: python seatbelt.py [database_path]"
        sys.exit(1)
