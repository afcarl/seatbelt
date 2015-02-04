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

var S = {};

(function($) {

    $.Triggerable = function() {
    };
    $.Triggerable.prototype._get_cbs = function(evtname) {
	return this["__"+evtname] || {};
    };
    $.Triggerable.prototype._set_cbs = function(evtname, cbs) {
	this["__"+evtname] = cbs;
    };
    $.Triggerable.prototype.bind = function(evtname, cb) {
	var cbs = this._get_cbs(evtname);
	var i = 1;
	while(i in cbs) {
            i++;
	}
	cbs[i] = cb;
	this._set_cbs(evtname, cbs);
	return i;
    };
    $.Triggerable.prototype.once = function(evtname, cb) {
	var that = this;
	var cbid = this.bind(evtname, function(res) {
            cb(res);
            that.unbind(evtname, cbid);
	});
    };
    $.Triggerable.prototype.unbind = function(evtname, cbid) {
	delete this._get_cbs(evtname)[cbid];
    };
    $.Triggerable.prototype.trigger = function(evtname, data) {
	var cbs = this._get_cbs(evtname);
	for(var key in cbs) {
            cbs[key](data, this);
	}
    };
    $.Triggerable.prototype.reset = function() {
	// Remove all events
	for(var key in this) {
            if(key.slice(0,2) === "__") {
		delete this[key];
            }
	}
    };
    $.Triggerable.prototype.connect = function(remote_obj, name, cb) {
	// Make a connection to a remote object; keep track of all such
	// connections so that they can be destroyed all at once.
	if(this.__connections === undefined) {
	    this.__connections = [];
	}
	var cbid = remote_obj.bind(name, cb);
	this.__connections.push([remote_obj, name, cbid]);
    }
    $.Triggerable.prototype.destroy = function() {
	// Remove all inbound connections
	(this.__connections || []).forEach(function(con_obj) {
	    con_obj[0].unbind(con_obj[1], con_obj[2]);
	});
	this.__connections = undefined;
    }

    $.Document = function(ego, doc) {
	$.Triggerable.call(this);
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
    $.Document.prototype = new $.Triggerable;
    $.Document.prototype.get_doc = function() {
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
    $.Document.prototype.set_doc = function(doc) {
        var changedKeys = [];
        for(var k in doc) {  // added and modified properties
            if (this[k] !== doc[k]) {
                this[k] = doc[k];
                changedKeys.push(k);
            }
        }
        for (var k in this) {  // deleted properties
            if(doc[k] === undefined && this.hasOwnProperty(k) && k.slice(0,2) !== '__' && k.substr(0,1) !== '_') {
                delete this[k];
                changedKeys.push(k);
            }
        }
        this._trigger_changes_for_keys(changedKeys);
    }
    $.Document.prototype.update_doc = function(doc) {
        var changedKeys = [];
        for(var k in doc) {
            if (this[k] !== doc[k]) {
                this[k] = doc[k];
                changedKeys.push(k);
            }
        }
        this._trigger_changes_for_keys(changedKeys);
    };
    $.Document.prototype._trigger_changes_for_keys = function(keys) {
        if (keys.length == 0) { return; }
        keys.forEach(function (k) { 
            this.trigger(k + "-change", this);
        }, this);
        this.trigger("change", this);
        if(this.__ego.get(this._id)) {
            this.__ego.trigger("change", this);
        }
    }
    $.Document.prototype.set = function(key, value) {
	if(this[key] !== value) {
            this[key] = value;
            this._trigger_changes_for_keys([key]);
            return true;
	};
	return false;
    };
    $.Document.prototype.set_foreign = function(key, value) {
	return this.set(key, value._id);
    };
    $.Document.prototype.get_foreign = function(key) {
	return this.__ego.get(this[key]);
    };
    $.Document.prototype.get_all_foreign = function(key) {
	var that = this;
	return this[key].map(function(x) { return that.__ego.get(x); });
    };
    $.Document.prototype.save = function(success, error) {
	var that = this;
	// Wrap the success function to update _rev property
	this.__ego.save(this.get_doc(), function(res) {
            that._rev = res.rev;
            if(success) {
		success();
            }
	}, error);
    };
    $.Document.prototype.deleteme = function(success, error) {
	this.__ego.remove(this._id, this._rev, success, error);
	this.trigger("delete");
    };
    $.Document.prototype.set_attachment = function(name, payload, content_type) {
	// payload should be base64-encoded
	if(this._attachments === undefined) {
            this._attachments = {};
	}
	this._attachments[name] = {
            content_type: content_type,
            data: payload};
    };
    $.Document.prototype.put_file_attachment = function(name, file, cb, progress_cb) {
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
    $.Document.prototype.remove_attachment = function(name) {
	delete this._attachments[name];
    };
    $.Document.prototype.get_attachment_url = function(name) {
	return this.__ego.db + this._id + "/" + name;
    }
    $.Document.prototype._make_id = function() {
	this._id = "id_" + ("" + Math.random()).slice(2);
    };
    $.Document.prototype.render_into = function(cb, $parent, $div) {
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

    $.Database = function(db) {
	$.Triggerable.call(this);

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
    $.Database.prototype = new $.Triggerable;
    $.Database.prototype.get = function(id) {
	return this._obj[id];
    };
    $.Database.prototype.items = function(sortfn) {
	var out = [];
	for(var k in this._obj) {
            out.push(this._obj[k]);
	}
        _sort_array(out,sortfn);
	return out;
    };
    $.Database.prototype.save = function(doc, success, error) {
	var that = this;

	if(this.socket) {
	    this.socket.send(JSON.stringify(doc));
	    // Fire success when we get the new doc back
	    // XXX: We have no idea if this is a success or not.
	    this.get(doc._id).once("_rev-change", function() {
		success({ok: true, rev: this.get(doc._id)._rev}); // XXX: better emulate couch success call?
	    }.bind(this));
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
    $.Database.prototype.remove = function(id, rev, success, error) {

	// XXX: Implement over web sockets--set __deleted=True or smth

	var that = this;
	this.trigger("delete", this.get(id));
	if(rev === undefined) {
            // Not a real, couch-backed object object
            delete this._obj[id];
            return;
	}

        if(this.socket) {
            // Send a delete message over websockets
            var doc = {"_id": id, "_rev": rev, "_deleted": true};
            this.socket.send(JSON.stringify(doc));
            // TODO: success/error callbacks
        }
        else {
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
        }
    };
    $.Database.prototype.register = function(obj) {
	this._obj[obj._id] = obj;
	this.trigger("create", obj);
    };
    $.Database.prototype.load = function(cb) {
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
    $.Database.prototype.script_load = function() {
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
    $.Database.prototype.checkpoint = function(id, rev) {
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
    $.Database.prototype.undo = function() {
	if(this._undocreate) {
            this.remove(this._undodoc._id, this._undodoc._rev);
	}
	else if(this._undodoc) {
            this.onchange(this._undodoc);
            this.save(this._undodoc);
	}
    }
    $.Database.prototype.local_changes = function() {
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
    $.Database.prototype.connect = function() {
	// Load documents and establish a websocket connection for changes.
	this.load(function() {

            // Derive web socket URL
            // TODO: crypto
            var url = "ws://" + window.location.host;
            console.log(this.db.slice(0,7));
            if(this.db.slice(0,7) == "http://") {
                console.log("remote websocket requested");
                url = "ws://" + this.db.split("http://")[1];
            }
	    // XXX: What are these cases?
            else if(this.db[0] == "/") {
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
    $.Database.prototype._ondocchange = function(doc) {
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
    $.Database.prototype.ondelete = function(doc) {
	if(this.get(doc._id)) {
            this.trigger("delete", this.get(doc._id));
            this.get(doc._id).trigger("delete");
            delete this._obj[doc._id];
	}
    };
    $.Database.prototype.oncreate = function(doc) {
	// side-effect of creation is SUPER_EGO.register
	var foo = new $.Document(this, doc);

	this._worldstate[doc._id] = doc;
    };
    $.Database.prototype.onchange = function(doc) {
	if(this.get(doc._id)) {
            this._obj[doc._id].update_doc(doc);
            // this.trigger("change", this.get(doc._id));
	}
    };
    $.Database.prototype.onload = function() {
	// called when all of the initial documents have been loaded
	this.trigger("load");
    };
    $.Database.prototype.logout = function(cb) {
	var xhr = new XMLHttpRequest();
	xhr.open("DELETE", "/_session", true);
	xhr.onload = function() {
            cb(JSON.parse(this.responseText));
	};
	xhr.send();
    };

    $.CollectionWatcher = function(collection) {
	$.Triggerable.call(this);
	if(!collection) {
            return;
	}

	this.collection = collection;
	this.connect(this.collection, "delete", this._delete.bind(this));
	this.connect(this.collection, "create", this._create.bind(this));
	this.connect(this.collection, "change", this._change.bind(this));
    }
    $.CollectionWatcher.prototype = new $.Triggerable;
    $.CollectionWatcher.prototype._create = function() { throw("not implemented");};
    $.CollectionWatcher.prototype._change = function() { throw("not implemented");};
    $.CollectionWatcher.prototype._delete = function() { throw("not implemented");};

    $.Subcollection = function(collection, filterfn, initial) {
	$.CollectionWatcher.call(this, collection);

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
    $.Subcollection.prototype = new $.CollectionWatcher;
    $.Subcollection.prototype._id = function(obj) {
	return obj._id || obj;
    }
    $.Subcollection.prototype._create = function(obj) {
	this._sorted = null;
	if(this.filterfn(obj)) {
            this._items[this._id(obj)] = obj;
            this.trigger("create", obj);
	}
    };
    $.Subcollection.prototype._delete = function(obj) {
	this._sorted = null;
	if(this._id(obj) in this._items) {
            delete this._items[this._id(obj)];
            this.trigger("delete", obj);
	}
    };
    $.Subcollection.prototype._change = function(obj) {
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
    $.Subcollection.prototype.get = function(id) {
	return this._items[id];
    };
    $.Subcollection.prototype.items = function(sortfn) {
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
		_sort_array(out, sortfn);
		this._sorted = out;
            }
            else {
		return out;
            }
	}
	return this._sorted;
    };
    $.Subcollection.prototype.ingest = function() {
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

    $.CollectionMap = function(collection, keyfn) {
	// Maintains a mapping of key -> [item], where keys are
	// returned by keyfn(item). A collection map is itself a
	// collection of keys -- items() returns a list of keys. get()
	// behaves a bit differently, and returns all of the items
	// within a key.
	this.keyfn = keyfn;

	this._mapping = {};     // key -> [item]
	this._revmapping = {};  // itemid -> [key] (for internal mgmt)

	$.CollectionWatcher.call(this, collection);

	this.ingest();
    };
    $.CollectionMap.prototype = new $.CollectionWatcher;
    $.CollectionMap.prototype._id = function(obj) {
	return (obj._id || obj);
    }
    $.CollectionMap.prototype.ingest = function() {
	this.collection.items().forEach(function(x) {
            this._create(x);
	}.bind(this));
    }
    $.CollectionMap.prototype._concat_kv = function(key, value) {
	// does *not* check if value already in key --that's a caller invariant
	this._mapping[key] = (this._mapping[key] || []).concat([value]);

	if(this._mapping[key].length === 1) {
            this.trigger("create", key);
	}
	else {
            this.trigger("change", key);
	}
    };
    $.CollectionMap.prototype._remove_kv = function(key, value) {
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
    $.CollectionMap.prototype._create = function(obj) {
	var keys = this.keyfn(obj);
	this._revmapping[this._id(obj)] = keys;
	var that = this;
	keys.forEach(function(key) {
            that._concat_kv(key, obj);
	});
    };
    $.CollectionMap.prototype._change = function(obj) {
	var that = this;
	var newkeys = this.keyfn(obj);
	var oldkeys = this._revmapping[this._id(obj)] || [];

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

	this._revmapping[this._id(obj)] = newkeys;
    };
    $.CollectionMap.prototype._delete = function(obj) {
	var that = this;
	this._revmapping[this._id(obj)].forEach(function(key) {
            that._remove_kv(key, obj);
	});
	delete this._revmapping[this._id(obj)];
    };
    $.CollectionMap.prototype.items = function(sortfn) {
	var out = [];
	for(var k in this._mapping) {
            out.push(k);
	}
	if(sortfn) {
            _sort_array(out, sortfn);
	}
	return out;
    };
    $.CollectionMap.prototype.get = function(key) {
	return this._mapping[key] || [];
    };

    $.CollectionView = function(collection, renderfn, child_tagname, sortby, $parent_el) {
	$.CollectionWatcher.call(this, collection);
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
    $.CollectionView.prototype = new $.CollectionWatcher;
    $.CollectionView.prototype._defaultrender = function(obj, $div) {
	$div.innerHTML = JSON.stringify(obj.get_doc());
    };
    $.CollectionView.prototype.render = function(obj, $div) {
	if(this.renderfn) { this.renderfn(obj, $div); }
	else { this._defaultrender(obj, $div); }
    };
    $.CollectionView.prototype.get = function(id) {
	return this._itemdivs[id];
    };
    $.CollectionView.prototype.ingest = function() {
	this.collection.items().forEach(function(obj) {
            this._create(obj, true);
	}.bind(this));
	this.sort();
    };
    $.CollectionView.prototype._id = function(obj) {
	return (obj._id || obj);
    }
    $.CollectionView.prototype._create = function(obj, nosort) {
	var $div = this._itemdivs[this._id(obj)] || document.createElement(this.child_tagname);
	this._itemdivs[this._id(obj)] = $div;
	this.$el.appendChild($div);
	this.render(obj, $div);
	if(!nosort) {
            this._position(obj);
	}
    };
    $.CollectionView.prototype._change = function(obj) {
	this.render(obj, this._itemdivs[this._id(obj)]);
	this._position(obj);
    };
    $.CollectionView.prototype._delete = function(obj) {
	this._itemdivs[this._id(obj)].innerHTML = "";
	this.$el.removeChild(this._itemdivs[this._id(obj)]);
	delete this._itemdivs[this._id(obj)];
    };
    $.CollectionView.prototype.sort = function() {
	if(this.sortby) {
            var items = this.collection.items(this.sortby);
            items.forEach(function(obj) {
		this.$el.appendChild(this._itemdivs[this._id(obj)]);
            }.bind(this));
	};
    };
    $.CollectionView.prototype._position = function(obj) {
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

    var _sort_array = function (array, fn) {
        if (!fn) { return array; }
        if (typeof fn == "string") {
            var k = fn;
            fn = function (a,b) {
                var ak = a[k] || "0", bk = b[k] || "0";
                return (ak > bk) ? 1 : (ak < bk) ? -1 : 0;
            };
        }
        array.sort(fn);
        return array;        
    };

})(S);
