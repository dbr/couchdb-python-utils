{
    "_id" : "_design/_search",
    "language":"javascript",
    "views": {
        "all" : {
            "map":"function(doc) { emit(doc._id, null); }"
            }
        }
}
