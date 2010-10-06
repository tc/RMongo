\name{dbInsertDocument}
\alias{dbInsertDocument}

\title{Insert a document into a MongoDB collection}

\description{
Insert a json character object into the mongodb collection.
}

\usage{
  dbInsertDocument(rmongo.object, collection, doc) 
}

\arguments{
  \item{rmongo.object}{The RMongo object.}
  \item{collection}{The name of the collection the document is being inserted into.}
  \item{doc}{A JSON string document. }
}

\seealso{
  \code{\link{dbGetQuery}}
}
\examples{
  mongo <- mongoDbConnect("test")
  output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar"}')
  dbDisconnect(mongo)
}
