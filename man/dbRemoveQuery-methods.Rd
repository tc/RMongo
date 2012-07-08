\name{dbRemoveQuery-methods}
\docType{methods}
\alias{dbRemoveQuery}
\alias{dbRemoveQuery-methods}
\alias{dbRemoveQuery,RMongo,character,character-method}

\title{Performing a delete MongoDB query}

\description{
Send a json query to mongodb. 
}

\usage{
  dbRemoveQuery(rmongo.object, collection, query)
}

\arguments{
  \item{rmongo.object}{The RMongo object.}
  \item{collection}{The name of the collection the query is being performed upon.}
  \item{query}{A JSON string query. See http://www.mongodb.org/display/DOCS/Advanced+Queries for more information on the MongoDB query language.}
}

\seealso{
  \code{\link{mongoDbConnect}}
}
\examples{
  mongo <- mongoDbConnect("test")
  output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar"}')  
  output <- dbRemoveQuery(mongo, 'test_data', '{"foo": "bar"}')
  print(output)
}

