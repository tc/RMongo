\name{dbGetDistinct-methods}
\docType{methods}
\alias{dbGetDistinct}
\alias{dbGetDistinct-methods}
\alias{dbGetDistinct,RMongo,character,character,missing-method}
\alias{dbGetDistinct,RMongo,character,character,character-method}

\title{Performing a distinct MongoDB query}

\description{
Send a json query to mongodb. 
The output is a data.frame object and will work properly only if the mongoDB collection contains primitive data types. It may not work properly if there are any embedded documents or arrays. 
}

\usage{
  dbGetDistinct(rmongo.object, collection, key, query)
}

\arguments{
  \item{rmongo.object}{The RMongo object.}
  \item{collection}{The name of the collection the query is being performed upon.}
  \item{key}{A JSON string query. See http://www.mongodb.org/display/DOCS/Advanced+Queries for more information on the MongoDB query language.}
  \item{query}{A JSON string query. See http://www.mongodb.org/display/DOCS/Advanced+Queries for more information on the MongoDB query language.}
}

\seealso{
  \code{\link{mongoDbConnect}}
}
\examples{
  mongo <- mongoDbConnect("test")
  output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar"}')
  output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar"}')
  output <- dbGetDistinct(mongo, 'test_data', '{"foo": "bar"}')
  print(output)
}

