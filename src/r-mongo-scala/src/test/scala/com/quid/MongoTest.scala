package com.quid

import com.mongodb.util.JSON
import org.junit.{Before, Assert, Test}
import com.mongodb.{BasicDBList, Mongo, DB, DBObject}

/**
 * 
 * User: @tommychheng
 * Date: Sep 24, 2010
 * Time: 9:36:41 AM
 *
 *
 */
class MongoTest{
  def clearTestDB{
    val m = new Mongo()
    val db = m.getDB("test")
    val collection = db.getCollection("test_data")
    collection.drop() //make sure the collection is empty
  }

  @Before
  def insertDocs{
    clearTestDB

    val m = new Mongo()
    val db = m.getDB("test")
    val collection = db.getCollection("test_data")

    List(""" {"foo": "bar", "size": 5} """,
         """ {"foo": "n1", "size": 10} """).foreach{ doc =>
        val docObject = JSON.parse(doc).asInstanceOf[DBObject]
        collection.insert(docObject)
    }
  }

  @Test
  def testDbShowCollections{
    val rMongo = new RMongo("test")

    val collectionName = "test_data"

    assert(rMongo.dbShowCollections().contains(collectionName))
  }

  @Test
  def testDbInsertDocument{
    clearTestDB

    val rMongo = new RMongo("test")
    val doc = """ {"_id": "foo", "foo": "bar", "size": 5} """

    val response = rMongo.dbInsertDocument("test_data", doc)
    Assert.assertEquals("ok", response)

    val duplicateResponse = rMongo.dbInsertDocument("test_data", doc)

    Assert.assertEquals("E11000 duplicate key error index: test.test_data.$_id_  dup key: { : \"foo\" }", duplicateResponse)
  }

  @Test
  def testDbRemoveQuery{
    clearTestDB

    val rMongo = new RMongo("test")
    val doc = """ {"_id": "foo", "foo": "bar", "size": 5} """

    val response = rMongo.dbInsertDocument("test_data", doc)
    Assert.assertEquals("ok", response)

    val removeResponse = rMongo.dbRemoveQuery("test_data", """ {"_id": "foo"} """)

    Assert.assertEquals("ok", removeResponse)
  }

  @Test
  def testDbGetQuery{
    val rMongo = new RMongo("test")
    val results = rMongo.dbGetQuery("test_data", """ {} """)
    val record = parsedFirstRecordFrom(results)

    Assert.assertEquals("\"bar\"", record.getOrElse("foo", ""))
  }

  @Test
  def testDbGetQueryWithKeys{
    val rMongo = new RMongo("test")
    val results = rMongo.dbGetQuery("test_data", """ {} """, """ {"foo": 1} """, 0, 100)
    println(results)
    val record = parsedFirstRecordFrom(results)

    Assert.assertEquals("\"bar\"", record.getOrElse("foo", ""))
    Assert.assertEquals("", record.getOrElse("size", ""))
  }

  @Test
  def testDbGetQueryWithEmptyCollection{
    val rMongo = new RMongo("test")
    val results = rMongo.dbGetQuery("empty_collection", "{}")

    Assert.assertEquals("", results)
  }

  @Test
  def testDbGetQuerySorting{
    val rMongo = new RMongo("test")
    val results = rMongo.dbGetQuery("test_data",
      """ { "$query": {}, "$orderby": { "foo": -1 } }} """)
    val record = parsedFirstRecordFrom(results)

    Assert.assertEquals("\"n1\"", record.getOrElse("foo", ""))
  }

  @Test
  def testDbGetQueryPaginate{
    val rMongo = new RMongo("test")
    val page1 = rMongo.dbGetQuery("test_data", """ {} """, 0, 1)
    val record1 = parsedFirstRecordFrom(page1)

    Assert.assertEquals("\"bar\"", record1.getOrElse("foo", ""))

    val page2 = rMongo.dbGetQuery("test_data", """ {} """, 1, 1)
    val record2 = parsedFirstRecordFrom(page2)

    Assert.assertEquals("\"n1\"", record2.getOrElse("foo", ""))
  }

  @Test
  def testToCsvOutput{
    val m = new Mongo()
    val db = m.getDB("test")
    val collection = db.getCollection("test_data")
    val query = "{}"
    val queryObject = JSON.parse(query).asInstanceOf[DBObject]
    val cursor = collection.find(queryObject)

    val results = RMongo.toCsvOutput(cursor)

    println(results)
  }

  def parsedFirstRecordFrom(results: String):Map[String, Any] = {
    val lines = results.split("\n").filter(_.size > 0)

    val keys = lines.head.split(RMongo.SEPARATOR )
    val entries = lines.drop(1)
    val entry = entries.headOption.getOrElse("").split(RMongo.SEPARATOR )

    keys.zip(entry).toMap
  }
}