\name{dbAuthenticate-methods}
\docType{methods}
\alias{dbAuthenticate}
\alias{dbAuthenticate-methods}
\alias{dbAuthenticate,RMongo,character,character-method}

\title{Authenticate with a username/password.}

\description{
This is an optional authenticate method.
}

\usage{
dbAuthenticate(rmongo.object, username, password)
}

\arguments{
  \item{rmongo.object}{RMongo object containing the database connection information.}
  \item{username}{Username}
  \item{password}{Password}
}

\seealso{
  \code{\link{dbAuthenticate}}
}
\examples{
  mongo <- mongoDbConnect("test")
  username = ""
  password = ""
  authenticated <- dbAuthenticate(mongo, username, password)
  dbDisconnect(mongo)
}

