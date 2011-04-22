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

\section{Overview}{
A typical workflow using RMongo:
  \enumerate{
    \item Connect to a mongodb database:
      \preformatted{
        mongo <- mongoDbConnect("test", "localhost", 27017)  
      }
    \item Insert a document into a collection:
      This will insert a document of '{"foo":"bar"}' into the "test_data" collection.
      \preformatted{
        output <- dbInsertDocument(mongo, "test_data", '{"foo": "bar"}')
      }
    \item Query a MongoDB collection:
     This will query for documents which have a "foo" attribute with the value of "bar". The return value will be a list of rjson objects or a converted data.frame depending on the format parameter. NOTE: nested documents and arrays may not work properly if you specify a data.frame.
      \preformatted{
        output <- dbGetQuery(mongo, "test_data", 
                                    '{"foo": "bar"}')
      }
      or if you want only a particular set of keys:
      \preformatted{
        output <- dbGetQueryForKeys(mongo, "test_data", 
                                    '{"foo": "bar"}', '{"foo": 1}')
      }
    \item Close the connection:
      \preformatted{
        dbDisconnect(mongo)
      }
    \item Large result sets:
    Set your java heap space to a large number to support loading more results into memory.
      \preformatted{
        options( java.parameters = "-Xmx2g" )
      }
  }
}

\author{
Tommy Chheng <tommy.chheng@gmail.com> 
http://tommy.chheng.com
}
\keyword{mongodb}
\keyword{mongo}
