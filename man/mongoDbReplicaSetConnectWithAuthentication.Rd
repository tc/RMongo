\name{mongoDbReplicaSetConnectWithAuthentication}
\alias{mongoDbReplicaSetConnectWithAuthentication}

\title{Connecting to a MongoDB Replica Set with new default Authentication SCARM-SHA1}

\description{
Connects to a Mongo DB Replica Set and creates a RMongo object to use with querying and inserting methods.
}

\usage{
mongoDbReplicaSetConnect(dbName, hosts="127.0.0.1:27017",username, password)
}

\arguments{
  \item{dbName}{Database name.}
  \item{hosts}{ Comma separated list of hosts member of the replica set. Each entry in the list must have the format <hostname>[:<port>] . If <port> is omitted, 27017 is used {optional}}
  \item{username}{Username as string.}
  \item{password}{Password as string.}
}

\examples{
  mongo <- mongoDbReplicaSetConnectWithAuthentication("test", "localhost:27017", "alice", "secret")
}
