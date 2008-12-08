
# couchdb-python-utils/doc_utils.py

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
