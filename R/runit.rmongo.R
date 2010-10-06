library('RUnit')
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
  output <- dbGetQuery(mongo, "test_data", '{"foo": "bar"}', format='json')
  dbDisconnect(mongo)
  
  checkEquals("bar", output[[1]]$foo)
}

test.dbGetQueryDataFrameFormat <- function(){
  mongo <- mongoDbConnect('test')
  output <- dbGetQuery(mongo, 'test_data', '{"foo":"bar"}', format='data.frame')
  dbDisconnect(mongo)
  
  checkEquals("bar", as.character(output[1,]$foo))
}

test.dbGetQuerySorting <- function(){
  #insert the records using r-mongo-scala project
  mongo <- mongoDbConnect("test")
  dbInsertDocument(mongo, "test_data", '{"foo": "bar"}')
  dbInsertDocument(mongo, "test_data", '{"foo": "newbar"}')
  
  output <- dbGetQuery(mongo, "test_data", '{ "$query": {}, "$orderby": { "foo": -1 } }}', format='json')
  dbDisconnect(mongo)
  
  checkEquals("newbar", output[[1]]$foo)
}


