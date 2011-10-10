\name{dbGetQuery-methods}
\docType{methods}
\alias{dbGetQuery}
\alias{dbGetQuery-methods}
\alias{dbGetQuery,RMongo,character,character-method}
\alias{dbGetQuery,RMongo,character,character,missing,missing-method}
\alias{dbGetQuery,RMongo,character,character,numeric,numeric-method}

\title{Performing a MongoDB query}

\description{
Send a json query to mongodb. 
The output is a data.frame object and will work properly only if the mongoDB collection contains primitive data types. It may not work properly if there are any embedded documents or arrays. 
}

\usage{
  dbGetQuery(rmongo.object, collection, query, skip=0, limit=1000) 
}

\arguments{
  \item{rmongo.object}{The RMongo object.}
  \item{collection}{The name of the collection the query is being performed upon.}
  \item{query}{A JSON string query. See http://www.mongodb.org/display/DOCS/Advanced+Queries for more information on the MongoDB query language.}
  \item{skip}{Offset the resultset. Can be used with limit to perform pagination.}
  \item{limit}{Limits the resultset size.}
}

\seealso{
  \code{\link{mongoDbConnect}}
}
\examples{
  mongo <- mongoDbConnect("test")
  output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar"}')  
  output <- dbGetQuery(mongo, 'test_data', '{"foo": "bar"}')
  print(output)
  
  output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar with spaces"}')
  output <- dbGetQuery(mongo, 'test_data', '{"foo": { "$regex": "bar*", "$options": "i"} }')
}
