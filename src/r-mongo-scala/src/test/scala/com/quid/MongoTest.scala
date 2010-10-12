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
    val doc = """ {"foo": "bar", "size": 5} """
    val docObject = JSON.parse(doc).asInstanceOf[DBObject]
    collection.insert(docObject)

    val doc1 = """ {"foo": "n1", "size": 10} """
    val docObject1 = JSON.parse(doc1).asInstanceOf[DBObject]
    collection.insert(docObject1)
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
  def testDbGetQuery{
    val rMongo = new RMongo("test")
    val results = rMongo.dbGetQuery("test_data", """ {} """)

    val jsonParsed = scala.util.parsing.json.JSON.parseFull(results)
    val records = jsonParsed.getOrElse(List()).asInstanceOf[List[Any]]
    val record = records.head.asInstanceOf[Map[String,Any]]

    Assert.assertEquals("bar", record.getOrElse("foo", ""))
  }

  @Test
  def testDbGetQueryWithEmptyCollection{
    val rMongo = new RMongo("test")
    val results = rMongo.dbGetQuery("empty_collection", """ {} """)

    Assert.assertEquals("[]", results)
  }
  @Test
  def testDbGetQuerySorting{
    val rMongo = new RMongo("test")
    val results = rMongo.dbGetQuery("test_data", """ { "$query": {}, "$orderby": { "foo": -1 } }} """)

    val jsonParsed = scala.util.parsing.json.JSON.parseFull(results)
    val records = jsonParsed.getOrElse(List()).asInstanceOf[List[Any]]
    val record = records.head.asInstanceOf[Map[String,Any]]

    Assert.assertEquals("n1", record.getOrElse("foo", ""))
  }
}