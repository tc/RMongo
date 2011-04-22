\name{mongoDbConnect}
\alias{mongoDbConnect}

\title{Connecting to a MongoDB database}

\description{
Creates a RMongo object to use with querying and inserting methods.
}

\usage{
mongoDbConnect(dbName, host="127.0.0.1", port=27017)
}

\arguments{
  \item{dbName}{Database name.}
  \item{host}{ Host name. Default to localhost: 127.0.0.1. You can specify a remote host. {optional}}
  \item{port}{ Port number. Default to mongodb's default port 27017. {optional}}
}

\examples{
  mongo <- mongoDbConnect("test", "127.0.0.1", 27017)
}
