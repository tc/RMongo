mongoDbConnect <- function(dbName, host="localhost", port=27017){
  rmongo <- new("RMongo", javaMongo = .jnew("com/quid/RMongo", dbName))
  rmongo
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
setGeneric("dbGetQuery", function(this, collection, query) standardGeneric("dbGetQuery"))
setMethod("dbGetQuery", signature(this="RMongo", collection="character", query="character"),
  function(this, collection, query){
    format <- "json"
    results <- .jcall(this@javaMongo, "S", "dbGetQuery", collection, query, format)
    if(format == "json"){
      jsonResults <- fromJSON(results)
      jsonResults
    }else if(format == "data.frame"){
     #convert to data.frame table
     results
    }
  }
)

setGeneric("dbDisconnect", function(this) standardGeneric("dbDisconnect"))
setMethod("dbDisconnect", signature(this="RMongo"),
  function(this){
    .jcall(this@javaMongo, "V", "close")
  }
)

