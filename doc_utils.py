"""

This script facilitates validating JSON and Python datastructures,
and then POST or PUTing them to a CouchDB server.

This is tested for the 0.8x branch, but not 0.9x.
It seems there are some small breaking changes (or
perhaps just bugs ;) in 0.9, so the script may require
some minor modifications before using it there.


### Usage

This script facilitates validating JSON and Python datastructures,
and then POST or PUTing them to a CouchDB server.

The script has one required and one or more optional positional
parameters:

    python doc_utils database-name
    python doc_utils database-name path/to/folder/or/file
    python doc_utils database-name doc1.py doc2.py doc3.json doc4.js

If you specify a folder, then the script will examine all
.py, .json, and .js files in the folder, and--if they meet
the format requirements--send them to CouchDB.

Invoke help at the command-line for more options

    python doc_utils --help

## Format for Individual Documents

For format for a `.py` file a dictionary.

    {
        '_id': '123123',
        'title': "hi"
    }

The format for `.js` and `.json` files is a JS dictionary.

    {
        "_id":"101",
        "desc":"and a description"
    }

In general the distinction between the two will typically be
quite small, but remember that JSON technically doesn't permit
a comma after the last element in an array/dictionary.


*   Individual documents are always PUT.
*   Thus they must have an ID specified.
*   They will not overwrite existing documents
    with the same ID.

## Format for Bulk Documents

The format for bulk documents is identical to that of
individual documents, but they must be a list/array
instead of a dict.

Python looks like:

    [
        {'_id': '123123','title': "hi"},
        {'title': "goodbye"}
    ]

Actually, thats exactly what the JSON version will look
like as well. Cool, eh?


*   Bulk documents are always POSTed, and thus do not
    require IDs (but may have them).
*   This will overwrite existing documents with
    the same IDs.

"""


import os, sys, httplib, time
from optparse import OptionParser
try:
    import simplejson as json
except ImportError:
    try:
        import json
    except ImportError:
        try:
            from django.utils import simplejson as json
        except:
            raise ImportError("Requires either simplejson, Python 2.6 or django.utils!")

__AUTHOR__ = "Will Larson (lethain@gmail.com)"
__LICENSE__ = "MIT License"

class DocUtils:
    def __init__(self, hostname, port, path, database, delay = 0.1, verbose = False):
        self.hostname = hostname
        self.port = port
        
        self.path = path
        self.database = database
        
        self.delay = delay
        self.verbose = verbose
        
        self.main()
    
    def main(self):
        # If path is list of arguments, run each of them
        if isinstance(self.path, list):
            for cur_dir in self.path:
                self.launcher(cur_dir)
        else:
            self.launcher(self.path)
    
    def launcher(self, path):
        if os.path.isdir(path):
            self.handle_directory(path)
        else:
            self.handle_file(path)

    def handle_directory(self, path):
        if self.verbose:
            print "Handling directory at %s" % path
        for filename in os.listdir(path):
            if not os.path.isdir(filename):
                self.handle_file(os.path.join(path,filename))
                time.sleep(self.delay)

    def handle_file(self, filename):
        _, ext = os.path.splitext(filename)
        if os.path.samefile(os.path.join(sys.path[0],sys.argv[0]), filename):
            if self.verbose:
                print "Ignoring %s (this script)." % filename
            return
        if ext == ".py":
            if self.verbose:
                print "Handling %s as Python filename." % filename
            f = open(filename,'r')
            data = f.read()
            f.close()
            self.handle_python(filename, data)
        elif ext in [".json", ".js"]:
            if self.verbose:
                print "Handling %s as JSON filename." % filename
            f = open(filename,'r')
            data = f.read()
            f.close()
            self.handle_json(filename, data)
        else:
            if self.verbose:
                print "Ignoring %s." % filename

    def handle_json(self, filename, string):
        self.handle_python(filename, json.loads(string), loaded=True)

    def handle_python(self, filename, data,loaded=False):
        if not loaded:
            try:
                data = eval(data)
            except SyntaxError, errormsg:
                print "Warning: Ignoring %s - does not meet requirements for CouchDB format (%s)" % (filename, errormsg)
                return
    
        if type(data) == dict:
            self.send_document(json.dumps(data), data['_id'])
        else:
            # put data into bulk submit format
            data = { "docs" : data }
            self.send_document(json.dumps(data))

    def send_document(self, data, docid="_bulk_docs"):
        if self.verbose:
            print "Sending _id: %s" % docid
            print "Data:", data
    
        path = "/%s/%s" % (self.database, docid)
        
        h = httplib.HTTPConnection(self.hostname, self.port)
        if docid == "_bulk_docs":
            h.request("POST",path,data)
            resp = h.getresponse()
        else:
            h.request("PUT",path,data)
            resp = h.getresponse()
        
        resp_text = resp.read()
        
        if int(resp.status) >= 400:
            print "Error (%s): %s" % (resp.status, resp_text)
        
        if self.verbose:
            print "Status:", resp.status
            print resp.read()
    
def main():
    DEFAULT_PATH = "./"
    
    parser = OptionParser("usage: python doc_utils.py database [path to docs]")
    parser.add_option("-p","--port",dest="port",type="int",
                       help="PORT for CouchDB",
                       default = 5984)
    parser.add_option("-n","--hostname",dest="hostname",
                       help="HOSTNAME for CouchDB",
                       default="localhost")
    parser.add_option("-v","--verbose",dest="verbose",
                       action="store_true",
                       default=False)
    parser.add_option("-d","--delay",dest="delay",type="float",
                      help="Delay between docs being sent",
                      default=0.1)

    (options, args) = parser.parse_args()
    if len(args) == 0:
        parser.error("must specify a database")
    elif len(args) == 1:
        options.database = args[0]
        options.path = DEFAULT_PATH
    elif len(args) >= 2:
        options.database = args[0]
        options.path = args[1:]
    else:
        parser.error("Invalid arguments")
    
    DocUtils(
        hostname = options.hostname,
        port = options.port,
        path = options.path,
        database = options.database,
        delay = options.delay,
        verbose = options.verbose
    )

if __name__ == '__main__':
    main()