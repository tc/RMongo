Sys.setenv(NOAWT = "1")

library("RUnit")
library("RMongo")
library('rJava')

test.dbInsertDocument <- function(){
  mongo <- mongoDbConnect("test")
  output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar"}')
  dbDisconnect(mongo)

  checkEquals("ok", output)
}

test.dbRemoveQuery <- function(){
  mongo <- mongoDbConnect("test")
  output <- dbRemoveQuery(mongo, "test_data", '{}')
  dbDisconnect(mongo)

  checkEquals("ok", output)  
}

test.dbGetQuery <- function(){
  mongo <- mongoDbConnect("test")
  output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar"}')
  output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar with spaces"}')
  output <- dbGetQuery(mongo, 'test_data', '{"foo": { "$regex": "bar*", "$options": "i"} }')
  dbRemoveQuery(mongo, "test_data", '{}')
  dbDisconnect(mongo)
  checkEquals("bar", as.character(output[1,]$foo))
  checkEquals("bar with spaces", as.character(output[2,]$foo))
}

test.dbGetQuerySkipAndLimit <- function(){
  mongo <- mongoDbConnect("test")
  output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar"}')
  output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar"}')
  output <- dbGetQuery(mongo, "test_data", '{"foo": "bar"}', 0, 1)
  dbRemoveQuery(mongo, "test_data", '{}')
  dbDisconnect(mongo)
  checkEquals(1, length(output[output$foo == 'bar', 1]))
}

test.dbGetQueryWithEmptyCollection <- function(){
  mongo <- mongoDbConnect('test')
  output <- dbGetQuery(mongo, 'test_data', '{"EMPTY": "EMPTY"}')
  dbRemoveQuery(mongo, "test_data", '{}')
  dbDisconnect(mongo)
  checkEquals(data.frame(), output)
}

test.dbGetQuerySorting <- function(){
  #insert the records using r-mongo-scala project
  mongo <- mongoDbConnect("test")
  dbInsertDocument(mongo, "test_data", '{"foo": "bar"}')
  dbInsertDocument(mongo, "test_data", '{"foo": "newbar"}')
  
  output <- dbGetQuery(mongo, "test_data", '{ "$query": {}, "$orderby": { "foo": -1 } }}')
  dbRemoveQuery(mongo, "test_data", '{}')
  dbDisconnect(mongo)
  
  checkEquals("newbar", as.character(output[1,]$foo))
}

test.dbGetQueryForKeys <- function(){
  mongo <- mongoDbConnect("test")
  output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar", "size": 5}')
  results <- dbGetQueryForKeys(mongo, "test_data", '{"foo": "bar"}', '{"foo": 1}')
  dbRemoveQuery(mongo, "test_data", '{}')
  dbDisconnect(mongo)

  checkEquals(TRUE, any(names(results) == "foo"))
  checkEquals(TRUE, any(names(results) != "size"))
}

test.dbInsertStructured <- function(){
  mongo <- mongoDbConnect("test")  
  output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar", "structured":  {"foo": "baz"}}')
  output <- dbGetQuery(mongo, "test_data", '{}')

  dbRemoveQuery(mongo, "test_data", '{}')
  dbDisconnect(mongo)
  checkEquals("{ \"foo\" : \"baz\"}", as.character(output[1,]$structured))
}

test.dbGetDistinct <- function(){
  mongo <- mongoDbConnect("test")
  dbInsertDocument(mongo, "test_data", '{"foo": "bar and baz"}')
  dbInsertDocument(mongo, "test_data", '{"foo": "baz and bar"}')
  
  output <- dbGetDistinct(mongo, "test_data", 'foo')
  dbRemoveQuery(mongo, "test_data", '{}')
  dbDisconnect(mongo)
  
  checkEquals("bar and baz", as.character(output[1]))
  checkEquals("baz and bar", as.character(output[2]))
}

test.dbAggregate <- function(){
  mongo <- mongoDbConnect("test")
  dbInsertDocument(mongo, "test_data", '{"foo": "bar", "size": 5 }')
  dbInsertDocument(mongo, "test_data", '{"foo": "nl", "size": 10 }')
  
  output <- dbAggregate(mongo, "test_data", c(' { "$project" : { "baz" : "$foo" } } ',
                                              ' { "$group" : { "_id" : "$baz" } } ',
                                               ' { "$match" : { "_id" : "bar" } } '))
  dbRemoveQuery(mongo, "test_data", '{}')
  dbDisconnect(mongo)
  
  # print(length(output))
  print(as.character(output[1]))

  checkEquals("{ \"_id\" : \"bar\"}", as.character(output[1]))
  # checkEquals("bar", as.character(output[1]))
}

test.dbInsertDocument()
test.dbRemoveQuery()
test.dbGetQuery()
test.dbGetQuerySkipAndLimit()
test.dbGetQueryWithEmptyCollection()
test.dbGetQuerySorting()
test.dbGetQueryForKeys()
test.dbInsertStructured()
test.dbGetDistinct()
test.dbAggregate()
