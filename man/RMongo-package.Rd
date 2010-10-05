\name{RMongo-package}
\alias{RMongo-package}
\alias{RMongo}
\docType{package}
\title{
MongoDB client for R.
}
\description{
The functions in this package allow you make queries with a MongoDB database from R.
}
\details{
  \tabular{ll}{
  Package: \tab RMongo\cr
  Type: \tab Package\cr
  Version: \tab 0.05\cr
  Date: \tab 2010-09-26\cr
  License: \tab GPL-3\cr
  LazyLoad: \tab yes\cr
  }
}

\section{Overview}{
A typical workflow using RMongo:

\enumerate{
  \item Connect to a mongodb database:
    \preformatted{
      mongo <- mongoDbConnect("test")
    }
  \item Insert a document into a collection:
    This will insert a document of '{"foo":"bar"}' into the "test_data" collection.
    \preformatted{
      output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar"}')
    }
  \item Query a MongoDB collection:
   This will query for a documents which have a "foo" attribute with the value of "bar". The return value will be a list of rjson objects or a converted data.frame depending on the format parameter. NOTE: nested documents and arrays may not work properly if you specify a data.frame.
    \preformatted{
      output <- dbGetQuery(mongo, "test_data", '{"foo": "bar"}', format='json')

      output <- dbGetQuery(mongo, "test_data", '{"foo": "bar"}', format='data.frame')

    }
  \item Close the connection:
    \preformatted{
      dbDisconnect(mongo)
    }
}

}

\author{
Tommy Chheng <tommy@quid.com>
}
\references{
rJava
rjson
}
\keyword{mongodb}
\keyword{mongo}
\seealso{
~~ \code{\link[rJava:rJava-package]{rJava}} ~~
}
\examples{
see examples in runit.rmongo.R
}
