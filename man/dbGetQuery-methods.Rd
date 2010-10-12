\name{dbGetQuery-methods}
\docType{methods}
\alias{dbGetQuery}
\alias{dbGetQuery-methods}
\alias{dbGetQuery,RMongo,character,character,character-method}

\title{Performing a MongoDB query}

\description{
Send a json query to mongodb.
}

\usage{
  dbGetQuery(rmongo.object, collection, query, format) 
}

\arguments{
  \item{rmongo.object}{The RMongo object.}
  \item{collection}{The name of the collection the query is being performed upon.}
  \item{query}{A JSON string query. See http://www.mongodb.org/display/DOCS/Advanced+Queries for more information on the MongoDB query language. }
  \item{format}{The format is 'json' which returns a rjson parsed list.The other option is 'data.frame' which will return a data.frame version of the resultset. WARNING: 'data.frame' will work properly only if the mongoDB collection contains primitive data types. It may not work properly if there are any embedded documents or arrays.  }
}

\seealso{
  \code{\link{mongoDbConnect}}
}
\examples{
  mongo <- mongoDbConnect("test")
  output <- dbGetQuery(mongo, 'test_data', '{"foo": "bar"}', format='json')
  output <- dbGetQuery(mongo, 'test_data', '{"foo":"bar"}', format='data.frame')
}
