\name{dbShowCollections-methods}
\docType{methods}
\alias{dbShowCollections}
\alias{dbShowCollections-methods}
\alias{dbShowCollections,RMongo-method}

\title{Shows a list of collections}

\description{
Shows a list of collections in the specified rmongo.object database.
}

\usage{
  dbShowCollections(rmongo.object) 
}

\arguments{
  \item{rmongo.object}{The RMongo object.}
}

\seealso{
  \code{\link{dbShowCollections-methods}}
}
\examples{
  mongo <- mongoDbConnect("test")
  dbShowCollections(mongo)
  dbDisconnect(mongo)
}

