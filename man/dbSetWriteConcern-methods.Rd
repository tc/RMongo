\name{dbSetWriteConcern-methods}
\docType{methods}
\alias{dbSetWriteConcern}
\alias{dbSetWriteConcern-methods}
\alias{dbSetWriteConcern,RMongo,numeric,numeric,logical,logical-method}

\title{Set write concern.}

\description{
This is an optional method to set the write concern for a given Mongo DB connection. By default the Mongo DB library does not check if a write (insert, update, remove) actually succeed.
}

\usage{
dbSetWriteConcern(rmongo.object, w, wtimeout, fsync, j)
}

\arguments{
  \item{rmongo.object}{RMongo object containing the database connection information.}
  \item{w}{Number of write to aknowledge. -1 Don't even report network errors. 0 Don't wait for acknowledgement from the server. 1 Wait for acknowledgement but don't wait for secondaries to replicate. 2+ Wait for one or more secondaries to also acknowledge.}
  \item{wtimeout}{How long to wait for slaves before failing. 0 indefinite. Greater than 0 milliseconds to wait.}
  \item{j}{Wait for group commit to journal.}
  \item{fsync}{Force fsync to disk.}
}

\examples{
  mongo <- mongoDbConnect("test")
  dbSetWriteConcern(mongo, 1, 0, FALSE, FALSE)
  dbDisconnect(mongo)
}
