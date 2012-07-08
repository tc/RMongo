\name{dbGetQueryForKeys-methods}
\docType{methods}
\alias{dbGetQueryForKeys}
\alias{dbGetQueryForKeys-methods}
\alias{dbGetQueryForKeys,RMongo,character,character,character-method}
\alias{dbGetQueryForKeys,RMongo,character,character,character,missing,missing-method}
\alias{dbGetQueryForKeys,RMongo,character,character,character,numeric,numeric-method}

\title{Performing a MongoDB query and only return back certain keys}

\description{
Send a json query to mongodb. 
The output is a data.frame object and will work properly only if the mongoDB collection contains primitive data types. It may not work properly if there are any embedded documents or arrays. 
}

\usage{
  dbGetQueryForKeys(rmongo.object, collection, query, keys, skip=0, limit=1000) 
}

\arguments{
  \item{rmongo.object}{The RMongo object.}
  \item{collection}{The name of the collection the query is being performed upon.}
  \item{query}{A JSON string query. See http://www.mongodb.org/display/DOCS/Advanced+Queries for more information on the MongoDB query language.}
  \item{keys}{A JSON string query. See http://www.mongodb.org/display/DOCS/Advanced+Queries for more information on the MongoDB query language.}
  \item{skip}{Offset the resultset. Can be used with limit to perform pagination.}
  \item{limit}{Limits the resultset size.}
}

\seealso{
  \code{\link{mongoDbConnect}}
}
\examples{
  mongo <- mongoDbConnect("test")
  output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar"}')  
  output <- dbGetQueryForKeys(mongo, 'test_data', '{"foo": "bar"}', '{"foo":1}')
  print(output)

}

