package com.quid

import com.mongodb.util.JSON
import com.mongodb.{Mongo, DBObject, DB, DBCursor}
import collection.mutable.ListBuffer

/**
 *
 * User: @tommychheng
 * Date: Sep 23, 2010
 * Time: 9:36:10 PM
 *
 *
 */

class RMongo(dbName: String, host: String, port: Int) {
  val m = new Mongo(host, port)
  val db = m.getDB(dbName)

  def this(dbName: String) = this (dbName, "localhost", 27017)


  def dbInsertDocument(collectionName: String, jsonDoc: String): String = {
    val dbCollection = db.getCollection(collectionName)

    val docObject = JSON.parse(jsonDoc).asInstanceOf[DBObject]
    val results = dbCollection.insert(docObject)

    if(results.getError == null) "ok" else results.getError
  }

  def dbGetQuery(collectionName: String, query: String): String = {
    val dbCollection = db.getCollection(collectionName)

    val queryObject = JSON.parse(query).asInstanceOf[DBObject]
    val cursor = dbCollection.find(queryObject).iterator

    val results = toJsonOutput(cursor)

    results
  }

  def toJsonOutput(cursor: java.util.Iterator[DBObject]): String = {
    val results = ListBuffer[String]()
    while (cursor.hasNext) {
      val item = cursor.next
      results.append(item.toString)
    }

    results.mkString("[", ",", "]")
  }

  def close() {
    m.close()
  }

  def main() {

  }
}
