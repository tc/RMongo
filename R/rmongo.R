#library('rJava')
#.jinit()
#.jaddClassPath("inst/java/r-mongo-scala-1.0-SNAPSHOT.jar")

setClass("RMongo", representation(javaMongo = "jobjRef"))

mongoDbConnect <- function(dbName, host="127.0.0.1", port=27017){
  rmongo <- new("RMongo", javaMongo = .jnew("rmongo/RMongo", dbName, host, as.integer(port)))
  rmongo
}

mongoDbReplicaSetConnect <- function(dbName, hosts="127.0.0.1:27017"){
  rmongo <- new("RMongo", javaMongo = .jnew("rmongo/RMongo", dbName, hosts, FALSE))
  dbDisconnect(rmongo)
  rmongo <- new("RMongo", javaMongo = .jnew("rmongo/RMongo", dbName, hosts, TRUE))
  rmongo
}

setGeneric("dbAuthenticate", function(rmongo.object, username, password) standardGeneric("dbAuthenticate"))
setMethod("dbAuthenticate", signature(rmongo.object="RMongo", username="character", password="character"),
   function(rmongo.object, username, password){
    results <- .jcall(rmongo.object@javaMongo, "Z", "dbAuthenticate", username, password)
    results
  }
)

setGeneric("dbSetWriteConcern", function(rmongo.object, w, wtimeout, fsync, j) standardGeneric("dbSetWriteConcern"))
setMethod("dbSetWriteConcern", signature(rmongo.object="RMongo", w="numeric", wtimeout="numeric", fsync="logical", j="logical"),
   function(rmongo.object, w, wtimeout, fsync, j){
    .jcall(rmongo.object@javaMongo, "V", "dbSetWriteConcern", as.integer(w), as.integer(wtimeout), fsync, j)
   }
)

setGeneric("dbShowCollections", function(rmongo.object) standardGeneric("dbShowCollections"))
setMethod("dbShowCollections", signature(rmongo.object="RMongo"),
   function(rmongo.object){
    results <- .jcall(rmongo.object@javaMongo, "[S", "dbShowCollections")
    results
  }
)
 
setGeneric("dbInsertDocument", function(rmongo.object, collection, doc) standardGeneric("dbInsertDocument"))
setMethod("dbInsertDocument", signature(rmongo.object="RMongo", collection="character", doc="character"),
  function(rmongo.object, collection, doc){
    results <- .jcall(rmongo.object@javaMongo, "S", "dbInsertDocument", collection, doc)
    results
  }
)

setGeneric("dbGetQueryForKeys", function(rmongo.object, collection, query, keys, skip=0, limit=1000) standardGeneric("dbGetQueryForKeys"))
setMethod("dbGetQueryForKeys", signature(rmongo.object="RMongo", collection="character", query="character", keys="character", skip='numeric', limit='numeric'),
  function(rmongo.object, collection, query, keys, skip, limit){
    results <- .jcall(rmongo.object@javaMongo, "S", "dbGetQuery", collection, query, keys, skip, limit)
    if(results == ""){
      data.frame()
    }else{      
      con <- textConnection(results)
      data.frame.results <- read.csv(con, sep="", stringsAsFactors=FALSE, quote="")
      close(con)

      data.frame.results
    }
  }
)

setMethod("dbGetQueryForKeys", signature(rmongo.object="RMongo", collection="character", query="character", keys="character", skip='missing', limit='missing'),
  function(rmongo.object, collection, query, keys, skip, limit){
    dbGetQueryForKeys(rmongo.object, collection, query, keys, 0, 1000)
  }
)

setGeneric("dbGetQuery", function(rmongo.object, collection, query, skip=0, limit=1000) standardGeneric("dbGetQuery"))
setMethod("dbGetQuery", signature(rmongo.object="RMongo", collection="character", query="character", skip='numeric', limit='numeric'),
  function(rmongo.object, collection, query, skip, limit){
    dbGetQueryForKeys(rmongo.object, collection, query, "{}", skip, limit)
  }
)

setMethod("dbGetQuery", signature(rmongo.object="RMongo", collection="character", query="character", skip='missing', limit='missing'),
  function(rmongo.object, collection, query, skip=0, limit=1000){
    dbGetQueryForKeys(rmongo.object, collection, query, "{}", skip, limit)
  }
)

setGeneric("dbDisconnect", function(rmongo.object) standardGeneric("dbDisconnect"))
setMethod("dbDisconnect", signature(rmongo.object="RMongo"),
  function(rmongo.object){
    .jcall(rmongo.object@javaMongo, "V", "close")
  }
)

setGeneric("dbRemoveQuery", function(rmongo.object, collection, query) standardGeneric("dbRemoveQuery"))
setMethod("dbRemoveQuery", signature(rmongo.object="RMongo", collection="character", query="character"),
  function(rmongo.object, collection, query){
    results <- .jcall(rmongo.object@javaMongo, "S", "dbRemoveQuery", collection, query)
    results
  }
)

setGeneric("dbGetDistinct", function(rmongo.object, collection, key, query="") standardGeneric("dbGetDistinct"))
setMethod("dbGetDistinct", signature(rmongo.object="RMongo", collection="character", key="character", query="missing"),
  function(rmongo.object, collection, key, query=""){
    dbGetDistinct(rmongo.object, collection, key, "")
  }
)

setMethod("dbGetDistinct", signature(rmongo.object="RMongo", collection="character", key="character", query="character"),
  function(rmongo.object, collection, key, query){
    results <- .jcall(rmongo.object@javaMongo, "S", "dbGetDistinct", collection, key, query)
    if(results == ""){
      vector(mode="character")
    }else{      
      con <- textConnection(results)
      data.frame.results <- read.table(con, sep="\n", stringsAsFactors=FALSE, quote="\"", header=FALSE)
      close(con)

      as.vector(t(data.frame.results))
    }
  }
)

setGeneric("dbAggregate", function(rmongo.object, collection, query="") standardGeneric("dbAggregate"))
setMethod("dbAggregate", signature(rmongo.object="RMongo", collection="character", query="character"),
  function(rmongo.object, collection, query){
    results <- .jcall(rmongo.object@javaMongo, "[S", "dbAggregate", collection, .jarray(query))
    results
  }
)
