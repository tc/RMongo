\name{mongoDbConnect}
\alias{mongoDbConnect}

\title{Connecting to a MongoDB database}

\description{
Creates a RMongo object to use with querying and inserting methods.
}

\usage{
mongoDbConnect(dbName, host="localhost", port=27017)
}

\arguments{
  \item{dbName}{Database name.}
  \item{host}{ Host name. Default to localhost. You can specify a remote host. {optional}}
  \item{port}{ Port number. Default to mongodb's default port 27017. {optional}}
}

\examples{
  mongo <- mongoDbConnect("test")
}
