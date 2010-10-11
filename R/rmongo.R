library('plyr')
library('rjson')

setClass("RMongo", representation(javaMongo = "jobjRef"))

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

setGeneric("dbInsertDocument", function(rmongo.object, collection, doc) standardGeneric("dbInsertDocument"))
setMethod("dbInsertDocument", signature(rmongo.object="RMongo", collection="character", doc="character"),
  function(rmongo.object, collection, doc){
    results <- .jcall(rmongo.object@javaMongo, "S", "dbInsertDocument", collection, doc)
    results
  }
)

#
# format can be json or data.frame
# json will return an rjson object
# data.frame will attempt to convert to flat data frame table.
setGeneric("dbGetQuery", function(rmongo.object, collection, query, format) standardGeneric("dbGetQuery"))
setMethod("dbGetQuery", signature(rmongo.object="RMongo", collection="character", query="character", format="character"),
  function(rmongo.object, collection, query, format="json"){
    results <- .jcall(rmongo.object@javaMongo, "S", "dbGetQuery", collection, query)
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

setGeneric("dbDisconnect", function(rmongo.object) standardGeneric("dbDisconnect"))
setMethod("dbDisconnect", signature(rmongo.object="RMongo"),
  function(rmongo.object){
    .jcall(rmongo.object@javaMongo, "V", "close")
  }
)



