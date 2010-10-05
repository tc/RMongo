library('plyr')
library('rjson')

mongoDbConnect <- function(dbName, host="localhost", port=27017){
  rmongo <- new("RMongo", javaMongo = .jnew("com/quid/RMongo", dbName))
  rmongo
}

flattenIdColumn <- function(rjson.list){
  lapply(rjson.list, function(doc){
    new.doc = doc
    new.doc$`_id` = doc$`_id`$`$oid`
    new.doc
  })
}

setClass("RMongo", representation(javaMongo = "jobjRef"))

setGeneric("dbInsertDocument", function(this, collection, doc) standardGeneric("dbInsertDocument"))
setMethod("dbInsertDocument", signature(this="RMongo", collection="character", doc="character"),
  function(this, collection, doc){
    results <- .jcall(this@javaMongo, "S", "dbInsertDocument", collection, doc)
    results
  }
)

#
# format can be json or data.frame
# json will return an rjson object
# data.frame will attempt to convert to flat data frame table.
setGeneric("dbGetQuery", function(this, collection, query, format) standardGeneric("dbGetQuery"))
setMethod("dbGetQuery", signature(this="RMongo", collection="character", query="character", format="character"),
  function(this, collection, query, format="json"){
    results <- .jcall(this@javaMongo, "S", "dbGetQuery", collection, query)
    json.results <- fromJSON(results)
    
    if(format == "json"){
      json.results
    }else if(format == "data.frame"){
      transformed.docs <- flattenIdColumn(json.results)
      data.frame.results <- ldply(transformed.docs, data.frame)
      data.frame.results
    }
  }
)

setGeneric("dbDisconnect", function(this) standardGeneric("dbDisconnect"))
setMethod("dbDisconnect", signature(this="RMongo"),
  function(this){
    .jcall(this@javaMongo, "V", "close")
  }
)



