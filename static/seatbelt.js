// basics
if(typeof Function.prototype.bind != 'function') {
    // http://stackoverflow.com/questions/7950395/typeerror-result-of-expression-near-bindthis-undefined-is-not-a
    Function.prototype.bind = function (bind) {
	var self = this;
	return function () {
	    var args = Array.prototype.slice.call(arguments);
	    return self.apply(bind || null, args);
	};
    };
}

var Triggerable = function() {
};
Triggerable.prototype._get_cbs = function(evtname) {
    return this["__"+evtname] || {};
};
Triggerable.prototype._set_cbs = function(evtname, cbs) {
    this["__"+evtname] = cbs;
};
Triggerable.prototype.bind = function(evtname, cb) {
    var cbs = this._get_cbs(evtname);
    var i = 1;
    while(i in cbs) {
        i++;
    }
    cbs[i] = cb;
    this._set_cbs(evtname, cbs);
    return i;
};
Triggerable.prototype.once = function(evtname, cb) {
    var that = this;
    var cbid = this.bind(evtname, function(res) {
        cb(res);
        that.unbind(evtname, cbid);
    });
};
Triggerable.prototype.unbind = function(evtname, cbid) {
    delete this._get_cbs(evtname)[cbid];
};
Triggerable.prototype.trigger = function(evtname, data) {
    var cbs = this._get_cbs(evtname);
    for(var key in cbs) {
        cbs[key](data, this);
    }
};
Triggerable.prototype.reset = function() {
    // Remove all events
    for(var key in this) {
        if(key.slice(0,2) === "__") {
            delete this[key];
        }
    }
};

var SuperModel = function(ego, doc) {
    Triggerable.call(this);
    if(!ego) {
        return;
    }
    this.__ego = ego;

    // When a doc is instantiated, we need all of the fields available
    // at once.
    // this.update_doc(doc);
    for(var key in doc) {
        this[key] = doc[key];
    }

    if(!doc._id) {
        // Generate an ID
        this._make_id();
    }

    this.__ego.register(this);
};
SuperModel.prototype = new Triggerable;
SuperModel.prototype.get_doc = function() {
    // Two trailing underscores is convention for an attribute to
    // be withheld from storage.

    var doc = {};
    for(var key in this) {
        if(this.hasOwnProperty(key) && key.slice(0,2) !== '__') {
            doc[key] = this[key];
        }
    }
    return doc;
};
SuperModel.prototype.update_doc = function(doc) {
    for(var k in doc) {
        this.set(k, doc[k]);
    };
};
SuperModel.prototype.set = function(key, value) {
    if(this[key] !== value) {
        this[key] = value;
        this.trigger(key + "-change");
        this.trigger("change");

        // fire change event in ego as well, if registered
        if(this.__ego.get(this._id)) {
            this.__ego.trigger("change", this);
        }
        return true;
    };
    return false;
};
SuperModel.prototype.set_foreign = function(key, value) {
    return this.set(key, value._id);
};
SuperModel.prototype.get_foreign = function(key) {
    return this.__ego.get(this[key]);
};
SuperModel.prototype.get_all_foreign = function(key) {
    var that = this;
    return this[key].map(function(x) { return that.__ego.get(x); });
};
SuperModel.prototype.save = function(success, error) {
    var that = this;
    // Wrap the success function to update _rev property
    this.__ego.save(this.get_doc(), function(res) {
        that._rev = res.rev;
        if(success) {
            success();
        }
    }, error);
};
SuperModel.prototype.deleteme = function(success, error) {
    this.__ego.remove(this._id, this._rev, success, error);
    this.trigger("delete");
};
SuperModel.prototype.set_attachment = function(name, payload, content_type) {
    // payload should be base64-encoded
    if(this._attachments === undefined) {
        this._attachments = {};
    }
    this._attachments[name] = {
        content_type: content_type,
        data: payload};
};
SuperModel.prototype.put_file_attachment = function(name, file, cb, progress_cb) {
    var xhr = new XMLHttpRequest();
    xhr.open("PUT", this.__ego.db + this._id + "/" + name + "?rev=" + this._rev, true);
    xhr.setRequestHeader("Content-Type", file.type);
    xhr.upload.onprogress = function(e) {
        if(e.lengthComputable && progress_cb) {
            progress_cb(e.loaded / e.total);
        }
    }
    xhr.onload = function() {
        var res = JSON.parse(this.responseText);
        if(cb) {
            cb(res);
        }
    }
    xhr.send(file);
};
SuperModel.prototype.remove_attachment = function(name) {
    delete this._attachments[name];
};
SuperModel.prototype._make_id = function() {
    this._id = "id_" + ("" + Math.random()).slice(2);
};
SuperModel.prototype.render_into = function(cb, $parent, $div) {
    // Takes a function that renders into the given $div; it will be
    // called repeatedly whenever the model updates until the $div is
    // no longer in the $parent, at which point signals will be
    // destroyed.
    var $parent = $parent || document.body;
    var $div = $div || document.createElement("div");

    var c_id = this.bind("change", function() {
        if(!($div && $div.parentElement)) {
            this.unbind("change", c_id);
            return;
        }
        cb($div);
    }.bind(this));

    if($div.parentElement !== $parent) {
        $parent.appendChild($div);
    }

    cb($div);
    return $div;
}

var SuperEgo = function(db) {
    Triggerable.call(this);

    // 'db' is a URI to the database root
    this.db = db || "db/";
    // insist on trailing slash
    if(this.db[this.db.length-1] != "/") {
        this.db += "/";
    }

    this._worldstate = {};      // id -> doc
    this._undodoc = null;

    // "private" variable with a key-value map of all objects
    this._obj = {};
};
SuperEgo.prototype = new Triggerable;
SuperEgo.prototype.get = function(id) {
    return this._obj[id];
};
SuperEgo.prototype.items = function(sortfn) {
    var out = [];
    for(var k in this._obj) {
        out.push(this._obj[k]);
    }
    out.sort(sortfn);
    return out;
};
SuperEgo.prototype.save = function(doc, success, error) {
    var that = this;

    if(this.socket) {
	// XXX: Need to implement success/error on websocket
	this.socket.send(JSON.stringify(doc));
    }
    else {
	var xhr = new XMLHttpRequest();
	xhr.open("PUT", this.db + encodeURIComponent(doc._id), true);
	xhr.setRequestHeader("Content-Type", "application/json");
	xhr.onload = function() {
            var res = JSON.parse(this.responseText);
            that.checkpoint(doc._id, res.rev, !!!doc._rev);

            if(res.ok && success) {
		success(res);
            }
            else if(!res.ok && error) {
		error(res);
            }
	}
	xhr.send(JSON.stringify(doc));
    }
};
SuperEgo.prototype.remove = function(id, rev, success, error) {

    // XXX: Implement over web sockets--set __deleted=True or smth

    var that = this;
    this.trigger("delete", this.get(id));
    if(rev === undefined) {
        // Not a real, couch-backed object object
        delete this._obj[id];
        return;
    }
    var xhr = new XMLHttpRequest();
    xhr.open("DELETE", this.db + id + "?rev=" + rev, true);
    xhr.onload = function() {
        var res = JSON.parse(this.responseText);
        if(res.ok) {
            that.checkpoint(id, null, false);

            if(success) {
                success(res);
            }
            delete that._obj[id];
        }
        else if(error) {
            error(res);
        }
    };
    xhr.send();
};
SuperEgo.prototype.register = function(obj) {
    this._obj[obj._id] = obj;
    this.trigger("create", obj);
};
SuperEgo.prototype.load = function(cb) {
    // Load all of the documents
    var xhr_all = new XMLHttpRequest();
    xhr_all.open("GET", this.db + "_all_docs?include_docs=true", true);
    var that = this;
    xhr_all.onload = function() {
        var rows = JSON.parse(this.responseText).rows;
        rows.forEach(function(row) {
            that.oncreate(row.doc);
        });

        that.onload();
        if(cb) {
            cb();
        }
    };
    xhr_all.send();
}
SuperEgo.prototype.script_load = function() {
    // Load all of the documents (for file://)
    // Assumes ROWS = {}
    var uri = this.db + "_all_docs.js";
    var $script = document.createElement("script");
    $script.setAttribute("src", uri);
    $script.onload = function() {
        var rows = DATA.rows;

        rows.forEach(function(row) {
            this.oncreate(row.doc);
        }.bind(this));

        this.onload();
    }.bind(this);
    document.body.appendChild($script);
}
SuperEgo.prototype.checkpoint = function(id, rev) {
    if(!this._worldstate) {
        console.log("no worldstate");
        return
    }
    if(id in this._worldstate) {
        // document update/delete
        this._undodoc = this._worldstate[id];
        this._undodoc._rev = rev;
        this._undocreate = false;
        if(!rev) {
            // Document was deleted -- we can replace without a revision ID
            delete this._undodoc["_rev"];
        }
   }
    else {
        // Document create
        this._undodoc = {"_id": id, "_rev": rev};
        this._undocreate = true;
    }
}
SuperEgo.prototype.undo = function() {
    if(this._undocreate) {
        this.remove(this._undodoc._id, this._undodoc._rev);
    }
    else if(this._undodoc) {
        this.onchange(this._undodoc);
        this.save(this._undodoc);
    }
}
SuperEgo.prototype.local_changes = function() {
    // One-off _changes parsing
    var url = this.db + "_changes";
    var xhr = new XMLHttpRequest();
    xhr.open("GET", url, true);
    xhr.onload = function() {
        xhr.responseText.split("\n").forEach(function(line) {
            if(line.length > 2) {
                var change = JSON.parse(line);
                this._ondocchange(change.doc);
            }
        }.bind(this))
        this.onload();
    }.bind(this);
    xhr.send();
}
SuperEgo.prototype.connect = function() {
    // Load documents and establish a websocket connection for changes.
    this.load(function() {

        // Derive web socket URL
        // TODO: crypto
        var url = "ws://" + window.location.host;

	// XXX: What are these cases?
        if(this.db[0] == "/") {
            url += this.db;
        }
        else {
	    // Strip the end of the url
	    var path = window.location.pathname;
	    if(path[path.length-1] !=="/") {
		var pathparts = path.split("/");
		path = pathparts.slice(0, pathparts.length-1).join("/") + "/";
	    }
            url += path + this.db;
        }
        url += "_changes/_ws";

        this.socket = new WebSocket(url);
        this.socket.onmessage = function(e) {
            if(typeof e.data == "string") {
                var res = JSON.parse(e.data);
                // Hmmm... maybe I should just make my own protocol.
                var doc = res.results[0].doc;

                // // reload page if ddoc changes
                // if(doc._id.indexOf("_design/") == 0) {
                //     window.location.reload();
                // }

                this._ondocchange(doc);

            }
        }.bind(this);
    }.bind(this));
}
SuperEgo.prototype.track = function() {

    // Tracking is the basis for history -- we will `checkmark'
    // objects when we see them from the server so that we can undo
    // our own reckless actions.

    load_db_with_code(this.db, null, {}, function(update_seq, rows) {
        // fire callbacks on existing docs
        rows.forEach(function(row) {
            if(!row.doc._id) {
                row.doc._id = row.id; // _users db response is inconsistent
            }
            this.oncreate(row.doc);
        }.bind(this));

        if(update_seq === undefined) {
            console.log("no update seq -> no realtime tracking");
            this.local_changes();
            return;
        }

        this.onload();

        // watch for additional changes
        this.watch(update_seq, function(doc) {
            // reload page if ddoc changes
            if(doc._id.indexOf("_design/") == 0) {
                console.log("going down for reload...");
                window.location.reload();
            }

            this._ondocchange(doc);

        }.bind(this));
    }.bind(this));
};
SuperEgo.prototype._ondocchange = function(doc) {
    if(doc._id in this._obj) {
        // Document modified
        if(doc._deleted) {
            this.ondelete(doc);

            delete this._worldstate[doc._id];
        }
        else {
            this.onchange(doc);

            this._worldstate[doc._id] = doc;
        }
    }
    else if(!doc._deleted){
        // New document
        this.oncreate(doc);

        this._worldstate[doc._id] = doc;
    }

};
SuperEgo.prototype.ondelete = function(doc) {
    if(this.get(doc._id)) {
        this.trigger("delete", this.get(doc._id));
        this.get(doc._id).trigger("delete");
        delete this._obj[doc._id];
    }
};
SuperEgo.prototype.oncreate = function(doc) {
    // side-effect of creation is SUPER_EGO.register
    var foo = new SuperModel(this, doc);

    this._worldstate[doc._id] = doc;
};
SuperEgo.prototype.onchange = function(doc) {
    if(this.get(doc._id)) {
        this._obj[doc._id].update_doc(doc);
        // this.trigger("change", this.get(doc._id));
    }
};
SuperEgo.prototype.onload = function() {
    // called when all of the initial documents have been loaded
    this.trigger("load");
};
SuperEgo.prototype.whoami = function(cb) {
    // query _session for user info
    var xhr = new XMLHttpRequest();
    xhr.open("GET", "/_session", true);
    xhr.onload = function() {
        cb(JSON.parse(this.responseText).userCtx);
    };
    xhr.send();
};
SuperEgo.prototype.auth = function(uname, pass, cb) {
    // query _session for user info
    var xhr = new XMLHttpRequest();
    xhr.open("POST", "/_session", true);
    xhr.setRequestHeader("Content-Type", "application/json");
    xhr.onload = function() {
        cb(JSON.parse(this.responseText));
    };
    xhr.send(JSON.stringify({"name": uname, "password": pass}));
};
SuperEgo.prototype.logout = function(cb) {
    var xhr = new XMLHttpRequest();
    xhr.open("DELETE", "/_session", true);
    xhr.onload = function() {
        cb(JSON.parse(this.responseText));
    };
    xhr.send();
};

var MAX_TIMEOUT = 60*3*1000; // 3min

SuperEgo.prototype.watch = function(update_seq, ondoc, timeout) {
    // low-level database tracking -- returns each change as a
    // json doc.
    //
    // unless you know what you're doing, you probably want to use
    // launch() or SuperEgo.track() instead of this.

    var that = this;

    // returns a tracking-id
    var tracking_id = get_next_tracking_id();
    var timeout = timeout || 300; // time to delay after getting changes
    var orig_timeout = timeout;
    var timeouttime = new Date().getTime()

    _tracking[tracking_id] = true;

    // Realtime database monitoring.
    (function(my_tracking_id) {
        function _next_change(update_seq) {
            if(!_tracking[my_tracking_id]){
                console.log("stop tracking");
                return;
            }
            var url = that.db + "_changes?since=" + update_seq + "&feed=longpoll&include_docs=true";
            var xhr = new XMLHttpRequest();
            xhr.open("GET", url, true);
            xhr.onload = function() {
                var res = this.responseText;
                var data;
                try {
                    data = JSON.parse(res);
                }
                catch( e ) {
                    // invalid JSON

                    // 1. if result is non-empty, then something's wrong
                    // (Unless it's a 504 Gateway Timeout)
                    if(res && xhr.status != 504) {
                        timeout = Math.min(MAX_TIMEOUT, timeout*2);
                        console.log("bad data, doubling timeout", res, timeout);
                    }
                    // 2. result is empty
                    else {
                        // (a) - if it came too quickly, there's a problem
                        if(new Date().getTime() - timeouttime < 1000) {
                            timeout = Math.min(MAX_TIMEOUT, timeout*2);
                            console.log("fast and bad data, doubling timeout", res, timeout);
                        }
                        // (b) - otherwise, empty response probably just proxy timeout
                        else {
                            console.log("empty data -- probably not a problem");
                        }
                    }
                }

                if(_tracking[my_tracking_id] && data && data.results) {
                    data.results.forEach(function(res) {
                        ondoc(res.doc);
                    });

                    // reset timeout
                    timeout = orig_timeout;
                }

                window.setTimeout(function(){
                    timeouttime = new Date().getTime();
                    _next_change((data && data.last_seq) || update_seq);
                }, timeout);
            };
            xhr.send();
        }
        _next_change(update_seq);
    })(tracking_id);
};
// -- LAUNCH --
var load_db = function(db, cb) {
    var xhr_get = new XMLHttpRequest();
    xhr_get.open("GET", db, true);
    xhr_get.onload = function() {

        var data = {};
        try {
            data = JSON.parse(this.responseText);
        }
        catch( e ) {
            // Invalid JSON -- we must be offline
            console.log("We seem to be read-only");
        }

        var all = function() {
            var xhr_all = new XMLHttpRequest();
            xhr_all.open("GET", db + "/_all_docs?include_docs=true", true);
            xhr_all.onload = function() {
                var rows = JSON.parse(this.responseText).rows;
                cb(data.update_seq, rows);
            };
            xhr_all.send();
        }

        if(data.error && data.reason == "no_db_file") {
            // need to create DB --let's try!
            var xhr_create = new XMLHttpRequest();
            xhr_create.open("PUT", db, true);
            xhr_create.onload = function() {
                // let's hope it worked...
                data.update_seq = 0;
                all();
            }
            xhr_create.send();
        }
        else {
            all()
        }
    };
    xhr_get.send();
}
var load_code = function(paths, cb) {
    var nrem = 0;
    var out = {};
    for(var id in paths) {
        nrem += 1;
        (function(key) {
            var xhr = new XMLHttpRequest();
            xhr.open("GET", paths[key], true);
            xhr.onload = function() {
                out[key] = this.responseText;
                nrem -= 1;
                if(nrem === 0) {
                    cb(out);
                }
            };
            xhr.send();
        })(id);
    }
};

var load_db_with_code = function(db, code_id, code_paths, cb) {
    // Load database.
    // If code_id is NOT in the database, add a row with all of the code.

    load_db(db, function(update_seq, rows) {
        if(code_id && rows.filter(function(r) { return r.id === code_id; }).length === 0) {
            load_code(code_paths, function(code) {
                code._id = code_id;
                code.type = "code";
                var row = {doc: code, id: code_id};
                rows.push(row);
                cb(update_seq, rows);
            });
        }
        else {
            cb(update_seq, rows);
        }
    });
};

var _last_tracking_id = 0;
function get_next_tracking_id() {
    _last_tracking_id++;
    return _last_tracking_id;
};
var _tracking = {};
function stop_tracking(id) {
    _tracking[id] = false;
};
function stop_all_tracking() {
    for(var key in _tracking) {
        stop_tracking(key);
    }
};

var start_session = function(db, code_id, code_paths) {
    stop_all_tracking();    // this allows launch to be called
    // repeatedly without weirdness.

    document.body.innerHTML = ''; // XXX: should call teardown FN
    // if a launch is running.

    load_db_with_code(db, code_id, code_paths, function(update_seq, rows) {
        // transform rows into a key-value store
        var docs = {};
        rows.forEach(function(row) {
            docs[row.doc._id] = row.doc;
        });

        // create an Ego object with all the loaded
        // documents. this is a bit tricky, because the ego should
        // store objects in the SuperModel-derivative form as
        // defined in the code_obj, but also we need to keep track
        // of raw JSON docs such that re-initialization can
        // proceed smoothly.
        var ego = new SuperEgo(db);

        (function(SUPER_EGO) {

            var running_code_obj = null;

            function run() {
                var code_obj = docs[code_id];
                if(!code_obj) {
                    // XXX: sensible error throwing
                    console.log("code not found", code_obj);
                    return;
                }

                if(running_code_obj) {
                    // TEARDOWN
                    if(running_code_obj.teardown) {
                        eval(running_code_obj.teardown);
                    }
                    else {
                        document.body.innerHTML = '';
                    }

                    // UPGRADE
                    if(code_obj.upgrade) {
                        eval(code_obj.upgrade);
                    }
                }
                running_code_obj = code_obj;

                set_css(running_code_obj.css || '');
                set_html(running_code_obj.html || '');

                // wipe super-ego
                // for(var key in SUPER_EGO._obj) {
                //     SUPER_EGO.remove(key);
                // }
                SUPER_EGO._obj = {};
                SUPER_EGO.reset();
                
                // PAYLOAD
                try {
                    eval(code_obj.js);
                }
                catch( e ) {
                    // XXX: would be nice to emit better debugging info.
                    console.log(e);
                    return;
                }

                // send all existing documents to SUPER_EGO
                for(var key in docs) {
                    SUPER_EGO.oncreate(docs[key]);
                }
                SUPER_EGO.onload();
            }

            // first time
            run();

            // start tracking
            SUPER_EGO.watch(update_seq, function(doc) {
                if(doc._id in docs) {
                    // Document modified
                    if(doc._deleted) {
                        delete docs[doc._id];
                        SUPER_EGO.ondelete(doc);
                    }
                    else {
                        docs[doc._id] = doc;
                        SUPER_EGO.onchange(doc);
                        if(doc._id === code_id) {
                            // HOT CODE PUSH!
                            run();
                        }
                    }
                }
                else {
                    // New document
                    docs[doc._id] = doc;
                    SUPER_EGO.oncreate(doc);
                }
            });
        })(ego);
    });
}
var set_html = function(h) {
    document.body.innerHTML = h;
}
var set_css = function(c) {
    var $el = document.getElementById("css") || document.createElement("style");
    $el.id = "css";
    $el.innerHTML = c;
    document.head.appendChild($el);
};

var force_login = function(ego, cb) {
    var $el = document.createElement("div");
    $el.id = "forcelogin";

    var $h2 = document.createElement("h2");
    $h2.innerHTML = "access restricted";
    $el.appendChild($h2);

    var $err = document.createElement("div");
    $err.className = "error";
    $el.appendChild($err);

    var $lab = document.createElement("span");
    $lab.innerHTML = "user";
    $el.appendChild($lab);
    var $uname = document.createElement("input");
    $el.appendChild($uname);
    $el.appendChild(document.createElement("br"));

    var $plab = document.createElement("span");
    $plab.innerHTML = "pass";
    $el.appendChild($plab);
    var $pass = document.createElement("input");
    $pass.setAttribute("type", "password");
    $el.appendChild($pass);
    $el.appendChild(document.createElement("br"));

    var $sub = document.createElement("button");
    $sub.innerHTML = "in";
    $el.appendChild($sub);

    var try_login = function() {
        ego.auth($uname.value, $pass.value, function(ret) {
            if(ret.name || ret.roles) {
                if(cb) { cb(ret); }
                document.body.removeChild($el);
            }
            else {
                $err.innerHTML = "invalid name/pass";
            }
        });
    }

    $sub.onclick = try_login;
    function key_try_auth(ev) {
        if(ev.keyCode == 13) {      // return
            try_login();
        }
    }
    $uname.addEventListener("keydown", key_try_auth);
    $pass.addEventListener("keydown", key_try_auth);

    document.body.appendChild($el);
}

var CollectionWatcher = function(collection) {
    Triggerable.call(this);
    if(!collection) {
        return;
    }

    this.collection = collection;

    // Keep track of all listeners for clean teardown.
    this._tracks = [];      // [{obj, evtname, cbid}]

    // Watch the `create,' `change,' and `delete signals.
    var that = this;
    function watch(evtname, fn) {
        that._tracks.push({
            obj: that.collection,
            evtname: evtname,
            cbid: that.collection.bind(evtname, fn)});
    }
    watch("delete", function(obj) { that._delete(obj);});
    watch("create", function(obj) { that._create(obj);});
    watch("change", function(obj) { that._change(obj);});
}
CollectionWatcher.prototype = new Triggerable;
CollectionWatcher.prototype.destroy = function() {
    // stop tracking
    this._tracks.forEach(function(tr) {
        tr.obj.unbind(tr.evtname, tr.cbid);
    });
};
CollectionWatcher.prototype._create = function() { throw("not implemented");};
CollectionWatcher.prototype._change = function() { throw("not implemented");};
CollectionWatcher.prototype._delete = function() { throw("not implemented");};

var Subcollection = function(collection, filterfn, initial) {
    CollectionWatcher.call(this, collection);

    this.filterfn = filterfn;

    // Unsorted set of items within the collection (id -> obj)
    this._items = {};

    if(initial) {
        // initial subset provided (eg from a CollectionSearch)
        var that = this;
        initial.forEach(function(item) {
            that._items[this._id(item)] = item;
        }.bind(this));
    }
    else {
        // Initialize _items from the parent collection
        this.ingest();
    }
};
Subcollection.prototype = new CollectionWatcher;
Subcollection.prototype._id = function(obj) {
    return obj._id || obj;
}
Subcollection.prototype._create = function(obj) {
    this._sorted = null;
    if(this.filterfn(obj)) {
        this._items[this._id(obj)] = obj;
        this.trigger("create", obj);
    }
};
Subcollection.prototype._delete = function(obj) {
    this._sorted = null;
    if(this._id(obj) in this._items) {
        delete this._items[this._id(obj)];
        this.trigger("delete", obj);
    }
};
Subcollection.prototype._change = function(obj) {
    this._sorted = null;
    var in_collection = this.filterfn(obj);
    if(!in_collection) {
        // possibly removed
        this._delete(obj);
    }
    else if(in_collection) {
        if(!(this._id(obj) in this._items)) {
            // added
            this._create(obj);
        }
        else {
            // changed
            this.trigger("change", obj);
        }
    }
};
Subcollection.prototype.get = function(id) {
    return this._items[id];
};
Subcollection.prototype.items = function(sortfn) {
    // XXX: the subcollection cache is particularly sinister because
    // the same subcollection may be accessed by multiple sort
    // functions. For the moment, I assume that they're all the same,
    // but this is incorrect.
    if(!this._sorted) {
        var mapping = this._items;
        var out = [];
        for(var k in mapping) {
            out.push(mapping[k]);
        }
        if(sortfn) {
            out.sort(sortfn);
            this._sorted = out;
        }
        else {
            return out;
        }
    }
    return this._sorted;
};
Subcollection.prototype.ingest = function() {
    // Build up a new map of {id->obj} and merge with the current
    // map, firing events as needed.

    var that = this;
    var newcoll = {};
    this.collection.items().forEach(function(obj) {
        if(that.filterfn(obj)) {
            newcoll[this._id(obj)] = obj;
        }
    }.bind(this));
    // Merge:
    // 1. Remove items that no longer belong
    this.items().forEach(function(obj) {
        if(!(this._id(obj) in newcoll)) {
            that._delete(obj);
        }
    }.bind(this));
    // 2. Add new items
    for(var id in newcoll) {
        if(!(id in this._items)) {
            this._create(newcoll[id]);
        }
    }
};

var CollectionMap = function(collection, keyfn) {
    // Maintains a mapping of key -> [item], where keys are
    // returned by keyfn(item). A collection map is itself a
    // collection of keys -- items() returns a list of keys. get()
    // behaves a bit differently, and returns all of the items
    // within a key.
    this.keyfn = keyfn;

    this._mapping = {};     // key -> [item]
    this._revmapping = {};  // itemid -> [key] (for internal mgmt)

    CollectionWatcher.call(this, collection);

    this.ingest();
};
CollectionMap.prototype = new CollectionWatcher;
CollectionMap.prototype.ingest = function() {
    this.collection.items().forEach(function(x) {
        this._create(x);
    }.bind(this));
}
CollectionMap.prototype._concat_kv = function(key, value) {
    // does *not* check if value already in key --that's a caller invariant
    this._mapping[key] = (this._mapping[key] || []).concat([value]);

    if(this._mapping[key].length === 1) {
        this.trigger("create", key);
    }
    else {
        this.trigger("change", key);
    }
};
CollectionMap.prototype._remove_kv = function(key, value) {
    // sister to _concat_kv
    this._mapping[key].splice(this._mapping[key].indexOf(value), 1);
    if(this._mapping[key].length === 0) {
        delete this._mapping[key];
        this.trigger("delete", key);
    }
    else {
        this.trigger("change", key);
    }
};
CollectionMap.prototype._create = function(obj) {
    var keys = this.keyfn(obj);
    this._revmapping[obj._id] = keys;
    var that = this;
    keys.forEach(function(key) {
        that._concat_kv(key, obj);
    });
};
CollectionMap.prototype._change = function(obj) {
    var that = this;
    var newkeys = this.keyfn(obj);
    var oldkeys = this._revmapping[obj._id] || [];

    // add new keys
    newkeys.forEach(function(key) {
        if(oldkeys.indexOf(key) < 0) {
            that._concat_kv(key, obj);
        }
    });

    // remove old keys
    oldkeys.forEach(function(key) {
        if(newkeys.indexOf(key) < 0) {
            that._remove_kv(key, obj);
        }
    });

    this._revmapping[obj._id] = newkeys;
};
CollectionMap.prototype._delete = function(obj) {
    var that = this;
    this._revmapping[obj._id].forEach(function(key) {
        that._remove_kv(key, obj);
    });
    delete this._revmapping[obj._id];
};
CollectionMap.prototype.items = function(sortfn) {
    var out = [];
    for(var k in this._mapping) {
        out.push(k);
    }
    if(sortfn) {
        out.sort(sortfn);
    }
    return out;
};
CollectionMap.prototype.get = function(key) {
    return this._mapping[key] || [];
};



var SuperMarket = function(collection, renderfn, child_tagname, sortby, $parent_el) {
    CollectionWatcher.call(this, collection);
    if(!collection) {
        return;
    }
    this.renderfn = renderfn;

    this.sortby = sortby;

    this.$el = $parent_el || document.createElement("div");
    this.child_tagname = child_tagname || "div";

    this._itemdivs = {};    // id -> $div

    // Initialize
    this.ingest();
};
SuperMarket.prototype = new CollectionWatcher;
SuperMarket.prototype._defaultrender = function(obj, $div) {
    $div.innerHTML = JSON.stringify(obj.get_doc());
};
SuperMarket.prototype.render = function(obj, $div) {
    if(this.renderfn) { this.renderfn(obj, $div); }
    else { this._defaultrender(obj, $div); }
};
SuperMarket.prototype.get = function(id) {
    return this._itemdivs[id];
};
SuperMarket.prototype.ingest = function() {
    this.collection.items().forEach(function(obj) {
        this._create(obj, true);
    }.bind(this));
    this.sort();
};
SuperMarket.prototype._id = function(obj) {
    return (obj._id || obj);
}
SuperMarket.prototype._create = function(obj, nosort) {
    var $div = this._itemdivs[this._id(obj)] || document.createElement(this.child_tagname);
    this._itemdivs[this._id(obj)] = $div;
    this.$el.appendChild($div);
    this.render(obj, $div);
    if(!nosort) {
        this._position(obj);
    }
};
SuperMarket.prototype._change = function(obj) {
    this.render(obj, this._itemdivs[this._id(obj)]);
    this._position(obj);
};
SuperMarket.prototype._delete = function(obj) {
    this._itemdivs[this._id(obj)].innerHTML = "";
    this.$el.removeChild(this._itemdivs[this._id(obj)]);
    delete this._itemdivs[this._id(obj)];
};
SuperMarket.prototype.sort = function() {
    if(this.sortby) {
        var items = this.collection.items(this.sortby);
        items.forEach(function(obj) {
            this.$el.appendChild(this._itemdivs[this._id(obj)]);
        }.bind(this));
    };
};
SuperMarket.prototype._position = function(obj) {
    // Position one item correctly (without re-appending everything)
    var items = this.collection.items(this.sortby);
    var idx = items.indexOf(obj);
    if(idx+1 >= items.length) {
        // append to end
        this.$el.appendChild(this.get(this._id(obj)));
    }
    else {
        var next = items[idx+1];
        this.$el.insertBefore(this.get(this._id(obj)), this.get(this._id(next)));
    }
}
