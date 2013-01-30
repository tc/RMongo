\name{dbAggregate-methods}
\docType{methods}
\alias{dbAggregate}
\alias{dbAggregate-methods}
\alias{dbAggregate,RMongo,character,character-method}

\title{Performing a MongoDB aggregate query}

\description{
Send a set of aggregate pipeline commands to mongodb. 
The output is a data.frame object and will work properly only if the mongoDB collection contains primitive data types. It may not work properly if there are any embedded documents or arrays. 
}

\usage{
  dbAggregate(rmongo.object, collection, query) 
}

\arguments{
  \item{rmongo.object}{The RMongo object.}
  \item{collection}{The name of the collection the query is being performed upon.}
  \item{query}{A vector of pipeline command in JSON format. See http://docs.mongodb.org/manual/reference/aggregation/ for more information on the MongoDB aggregation framework.}
}

\seealso{
  \code{\link{mongoDbConnect}}
}
\examples{
  mongo <- mongoDbConnect("test")
  # insert two records
  dbInsertDocument(mongo, "test_data", '{"foo": "bar", "size": 5 }')
  dbInsertDocument(mongo, "test_data", '{"foo": "nl", "size": 10 }')
  output <- dbAggregate(mongo, "test_data", c(' { "$project" : { "baz" : "$foo" } } ',
                                              ' { "$group" : { "_id" : "$baz" } } ',
                                              ' { "$match" : { "_id" : "bar" } } '))
  print(output)
}
