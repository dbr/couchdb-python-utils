"""

This is tested for the 0.8x branch, but not 0.9x.
It seems there are some small breaking changes (or
perhaps just bugs ;) in 0.9, so the script may require
some minor modifications before using it there.


### Usage

This script facilitates validating JSON and Python datastructures,
and then POST or PUTing them to a CouchDB server.

The script has one required and one optional positional parameter:

    python doc_utils database-name
    python doc_utils database-name path/to/folder/or/file

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
            raise "Requires either simplejson, Python 2.6 or django.utils!"

__AUTHOR__ = "Will Larson (lethain@gmail.com)"
__LICENSE__ = "MIT License"


HOSTNAME = 'localhost'
PORT = 5984
DATABASE = None
PATH = "./"
VERBOSE = False
DELAY_BETWEEN_SENDS = 0.1

def handle_directory(path):
    global VERBOSE
    global DELAY_BETWEEN_SENDS
    if VERBOSE:
        print "Handling directory at %s" % path
    for file in os.listdir(path):
        if not os.path.isdir(file):
            handle_file(os.path.join(path,file))
            time.sleep(DELAY_BETWEEN_SENDS)

def handle_file(file):
    global VERBOSE
    _, ext = os.path.splitext(file)
    if os.path.samefile(os.path.join(sys.path[0],sys.argv[0]), file):
        if VERBOSE:
            print "Ignoring %s (this script)." % file
        return
    if ext == ".py":
        if VERBOSE:
            print "Handling %s as python file." % file
        f = open(file,'r')
        data = f.read()
        f.close()
        handle_python(file, data)
    elif ext in [".json", ".js"]:
        if VERBOSE:
            print "Handling %s as JSON file." % file
        f = open(file,'r')
        data = f.read()
        f.close()
        handle_json(file, data)
    else:
        if VERBOSE:
            print "Ignoring %s." % file

def handle_json(file, str):
    handle_python(file, json.loads(str),loaded=True)

def handle_python(file, data,loaded=False):
    if not loaded:
        try:
            data = eval(data)
        except:
            print "%s does not meet requirements for CouchDB format." % file
            return
    
    if type(data) == dict:
        send_document(json.dumps(data), data['_id'])
    else:
        # put data into bulk submit format
        data = { "docs" : data }
        send_document(json.dumps(data))

def send_document(data, id="_bulk_docs"):
    global VERBOSE
    global HOSTNAME
    global PORT
    global DATABASE
    if VERBOSE:
        print "Sending _id: %s" % id
        print "Data:", data
    
    path = "/%s/%s" % (DATABASE,id)
    
    h = httplib.HTTPConnection(HOSTNAME, PORT)
    if id == "_bulk_docs":
        h.request("POST",path,data)
        resp = h.getresponse()
    else:
        h.request("PUT",path,data)
        resp = h.getresponse()
    if VERBOSE:
        print "Status:", resp.status
    
def main():
    global DATABASE
    global PATH
    global PORT
    global HOSTNAME
    global VERBOSE
    parser = OptionParser("usage: python doc_utils.py database path?")
    parser.add_option("-p","--port",dest="port",
                       help="PORT for CouchDB",
                       metavar="PORT")
    parser.add_option("-n","--hostname",dest="hostname",
                       help="HOSTNAME for CouchDB",
                       metavar="HOSTNAME")
    parser.add_option("-v","--verbose",dest="verbose",
                       action="store_true", default=False,
                       metavar="VERBOSE")
    (options, args) = parser.parse_args()
    if len(args) < 1:
        parser.error("must specify a database")
    elif len(args) == 1:
        DATABASE = args[0]
    else:
        DATABASE = args[0]
        PATH = args[1]


    print options, args

    PORT = options.port if options.port else 5984
    HOSTNAME = options.hostname if options.hostname else 'localhost'
    VERBOSE = int(options.verbose) if options.verbose else False
    
    # use specified path
    if os.path.isdir(PATH):
        handle_directory(PATH)
    else:
        handle_file(PATH)

if __name__ == '__main__':
    main()
