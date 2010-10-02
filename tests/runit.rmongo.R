library('RUnit')
source('../R/rmongo.R', chdir=TRUE)

test.dbInsertDocument <- function(){
  mongo <- mongoDbConnect("test")
  output <- dbInsertDocument(mongo, "test_data", "{\"foo\": \"bar\"}")
  dbDisconnect(mongo)

  checkEquals("ok", output)
}

test.dbGetQuery <- function(){
  #insert the records using r-mongo-scala project
  mongo <- mongoDbConnect("test")
  output <- dbGetQuery(mongo, "test_data", "{\"foo\": \"bar\"}")
  dbDisconnect(mongo)
  
  checkEquals("bar", output[[1]]$foo)
}
