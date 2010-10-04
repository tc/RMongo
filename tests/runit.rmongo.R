library('RUnit')
library('rJava')
library('rjson')

.jinit()
.jaddClassPath('../inst/java/r-mongo-scala-1.0-SNAPSHOT.jar')

source('../R/rmongo.R', chdir=TRUE)

test.dbInsertDocument <- function(){
  mongo <- mongoDbConnect("test")
  output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar"}')
  dbDisconnect(mongo)

  checkEquals("ok", output)
}

test.dbGetQuery <- function(){
  #insert the records using r-mongo-scala project
  mongo <- mongoDbConnect("test")
  output <- dbGetQuery(mongo, "test_data", '{"foo": "bar"}')
  dbDisconnect(mongo)
  
  checkEquals("bar", output[[1]]$foo)
}

test.dbGetQuerySorting <- function(){
 #insert the records using r-mongo-scala project
  mongo <- mongoDbConnect("test")
  dbInsertDocument(mongo, "test_data", '{"foo": "bar"}')
  dbInsertDocument(mongo, "test_data", '{"foo": "newbar"}')
  
  output <- dbGetQuery(mongo, "test_data", '{ "$query": {}, "$orderby": { "foo": -1 } }}')
  dbDisconnect(mongo)
  
  checkEquals("newbar", output[[1]]$foo)
}
