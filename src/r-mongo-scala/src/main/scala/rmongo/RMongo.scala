package rmongo

import com.mongodb.util.JSON
import collection.mutable.ListBuffer
import collection.JavaConversions._
import com.mongodb._
import java.util.HashSet
import java.util.Iterator

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

  def this(dbName: String) = this (dbName, "127.0.0.1", 27017)

  def dbAuthenticate(username:String, password:String):Boolean = {
    db.authenticate(username, password.toCharArray)
  }

  def dbShowCollections():Array[String] = {
    val names = db.getCollectionNames().map(name => name)

    names.toArray
  }

  def dbInsertDocument(collectionName: String, jsonDoc: String): String = {
    dbModificationAction(collectionName, jsonDoc, _.insert(_))
  }

  def dbRemoveQuery(collectionName: String, query: String): String = {
    dbModificationAction(collectionName, query, _.remove(_))
  }

  def dbModificationAction(collectionName: String,
                          query: String,
                          action:(DBCollection, DBObject) => WriteResult): String = {
    val dbCollection = db.getCollection(collectionName)

    val queryObject = JSON.parse(query).asInstanceOf[DBObject]
    val results = action(dbCollection, queryObject)

    if(results.getError == null) "ok" else results.getError
  }

  def dbGetQuery(collectionName: String, query: String): String = {
    dbGetQuery(collectionName, query, 0, 1000)
  }

  def dbGetQuery(collectionName: String, query: String,
                                  skipNum:Double, limitNum:Double):String = {
    dbGetQuery(collectionName, query, skipNum.toInt, limitNum.toInt)
  }

  def dbGetQuery(collectionName: String, query: String,
                         skipNum:Int, limitNum:Int): String = {
    dbGetQuery(collectionName, query, "{}", skipNum, limitNum)
 }

  def dbGetQuery(collectionName:String, query:String, keys:String): String = {
    dbGetQuery(collectionName, query, keys, 0, 1000)
  }

  def dbGetQuery(collectionName: String, query: String, keys:String,
                                  skipNum:Double, limitNum:Double):String = {
    dbGetQuery(collectionName, query, keys, skipNum.toInt, limitNum.toInt)
  }

  def dbGetQuery(collectionName: String, query: String, keys: String,
                         skipNum:Int, limitNum:Int): String = {
    val dbCollection = db.getCollection(collectionName)

    val queryObject = JSON.parse(query).asInstanceOf[DBObject]
    val keysObject = JSON.parse(keys).asInstanceOf[DBObject]
    val cursor = dbCollection.find(queryObject, keysObject).skip(skipNum).limit(limitNum)

    val results = RMongo.toCsvOutput(cursor)

    results
  }
  
  def dbGetDistinct(collectionName: String, key: String): String = {
    dbGetDistinct(collectionName, key, "")
  }
  
  def dbGetDistinct(collectionName: String, key: String, query: String): String = {
    val dbCollection = db.getCollection(collectionName)

    val queryObject = JSON.parse(query).asInstanceOf[DBObject]
    val distinctResults = dbCollection.distinct(key, queryObject).iterator
    val results = ListBuffer[String]()

    while (distinctResults.hasNext) {
      val item = distinctResults.next
      results.append("\"" + item.toString.replaceAll("\n", "\\n") + "\"")
    }

    results.mkString("\n")
  }

  def close() {
    m.close()
  }

  def main() {

  }
}

object RMongo{
  val SEPARATOR = ""

  def toJsonOutput(cursor:DBCursor): String = {
    val results = ListBuffer[String]()
    while (cursor.hasNext) {
      val item = cursor.next
      results.append(item.toString)
    }

    results.mkString("[", SEPARATOR, "]")
  }

  def toCsvOutput(cursor: DBCursor): String = {
    
    if(cursor.hasNext == false) return ""
   
    val results = ListBuffer[String]()
    
    if (cursor.getKeysWanted != null && cursor.getKeysWanted.keySet.size != 0) {
        /* If fields were specified (save time) */
        val keysWanted = cursor.getKeysWanted
        keysWanted.put("_id", 1)
        val keys = keysWanted.keySet.toArray(new Array[String](keysWanted.keySet.size))
        results.append(keys.mkString(SEPARATOR)) 
        while (cursor.hasNext) {
            results.append(csvRowFromDBObject(keys, cursor.next))
        }
    } else {
        /* Try to find set of desired keys by scanning cursor */
        val keysWanted = new HashSet[String]()
        val ccursor:DBCursor = cursor.copy
        while(ccursor.hasNext == true) {
            keysWanted.addAll(ccursor.next.keySet)
        }
        val keys = keysWanted.toArray(new Array[String](keysWanted.size))
        results.append(keys.mkString(SEPARATOR)) 
        while (cursor.hasNext) {
            results.append(csvRowFromDBObject(keys, cursor.next))
        }
    }

    results.mkString("\n")
  }

  def csvRowFromDBObject(keys:Array[String], item:DBObject):String ={

    keys.map{k =>
      val value = item.get(k)

      if(value != null)
        value.toString.replaceAll("\"", "\\\"")
      else
        "" }.mkString(SEPARATOR)
  }
}
