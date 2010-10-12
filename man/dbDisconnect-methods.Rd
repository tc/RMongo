\name{dbDisconnect-methods}
\docType{methods}
\alias{dbDisconnect}
\alias{dbDisconnect-methods}
\alias{dbDisconnect,RMongo-method}

\title{Disconnect from the MongoDB database}

\description{
This will close the connect from the MongoDB database.
}

\usage{
dbDisconnect(rmongo.object)
}

\arguments{
  \item{rmongo.object}{RMongo object containing the database connection information.}
}

\seealso{
  \code{\link{mongoDbConnect}}
}
\examples{
  mongo <- mongoDbConnect("test")
  dbDisconnect(mongo)
}
