library("RUnit")
library("RMongo")
library('rJava')

test.dbInsertDocument <- function(){
  mongo <- mongoDbConnect("test")
  output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar"}')
  dbDisconnect(mongo)

  checkEquals("ok", output)
}

test.dbGetQuery <- function(){
  mongo <- mongoDbConnect("test")
  output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar"}')
  output <- dbGetQuery(mongo, "test_data", '{"foo": "bar"}')
  dbDisconnect(mongo)
  checkEquals("bar", as.character(output[1,]$foo))
}

test.dbGetQuerySkipAndLimit <- function(){
  mongo <- mongoDbConnect("test")
  output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar"}')
  output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar"}')
  output <- dbGetQuery(mongo, "test_data", '{"foo": "bar"}', 0, 1)
  dbDisconnect(mongo)
  checkEquals(1, length(output[output$foo == 'bar', 1]))
}

test.dbGetQueryWithEmptyCollection <- function(){
  mongo <- mongoDbConnect('test')
  output <- dbGetQuery(mongo, 'test_data', '{"EMPTY": "EMPTY"}')
  dbDisconnect(mongo)
  checkEquals(data.frame(), output)
}

test.dbGetQuerySorting <- function(){
  #insert the records using r-mongo-scala project
  mongo <- mongoDbConnect("test")
  dbInsertDocument(mongo, "test_data", '{"foo": "bar"}')
  dbInsertDocument(mongo, "test_data", '{"foo": "newbar"}')
  
  output <- dbGetQuery(mongo, "test_data", '{ "$query": {}, "$orderby": { "foo": -1 } }}')
  dbDisconnect(mongo)
  
  checkEquals("newbar", as.character(output[1,]$foo))
}

test.dbGetQueryForKeys <- function(){
  mongo <- mongoDbConnect("test")
  output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar", "size": 5}')
  results <- dbGetQueryForKeys(mongo, "test_data", '{"foo": "bar"}', '{"foo": 1}')
  dbDisconnect(mongo)

  checkEquals(TRUE, any(names(results) == "foo"))
  checkEquals(TRUE, any(names(results) != "size"))
}

test.dbInsertStructured <- function(){
  mongo <- mongoDbConnect("test")  
  output <- dbInsertDocument(mongo, "test_data_s", '{"foo": "bar", "structured":  {"foo": "baz"}}')
  output <- dbGetQuery(mongo, "test_data_s", '{}')

  dbDisconnect(mongo)
  checkEquals("{ \"foo\" : \"baz\"}", as.character(output[1,]$structured))
}

test.dbInsertDocument()
test.dbGetQuery()
test.dbGetQuerySkipAndLimit()
test.dbGetQueryWithEmptyCollection()
test.dbGetQuerySorting()
test.dbGetQueryForKeys()
test.dbInsertStructured()
