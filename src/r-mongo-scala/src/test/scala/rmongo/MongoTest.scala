package rmongo

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
  val m = new Mongo()
  val db = m.getDB("test")
  val collection = db.getCollection("test_data")

  def clearTestDB{
    collection.drop() //make sure the collection is empty
  }

  @Before
  def insertDocs{
    clearTestDB

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
  def testDbReplicaSetInsertDocument{
    clearTestDB

    val rMongo = new RMongo("test", "localhost", true)
    val doc = """ {"_id": "foo", "foo": "bar", "size": 5} """

    val response = rMongo.dbInsertDocument("test_data", doc)
    Assert.assertEquals("ok", response)

    val duplicateResponse = rMongo.dbInsertDocument("test_data", doc)
    var errMsg = """E11000 duplicate key error index: test.test_data.$_id_ dup key: { : "foo" }"""
    Assert.assertEquals(errMsg, duplicateResponse)
  }

  @Test
  def testDbInsertDocument{
    clearTestDB

    val rMongo = new RMongo("test")
    val doc = """ {"_id": "foo", "foo": "bar", "size": 5} """

    val response = rMongo.dbInsertDocument("test_data", doc)
    Assert.assertEquals("ok", response)

    val duplicateResponse = rMongo.dbInsertDocument("test_data", doc)
    var errMsg = """E11000 duplicate key error index: test.test_data.$_id_ dup key: { : "foo" }"""
    Assert.assertEquals(errMsg, duplicateResponse)
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

    Assert.assertEquals("bar", record.getOrElse("foo", ""))
  }

  @Test
  def testDbGetQueryInner{
    List(""" {"foo": "n1", "inner-parent": {"inner": 5} } """).foreach{ doc =>
        val docObject = JSON.parse(doc).asInstanceOf[DBObject]
        collection.insert(docObject)
    }

    val rMongo = new RMongo("test")
    val results = rMongo.dbGetQuery("test_data", """ {"inner-parent": {"inner": 5}} """)
    val record = parsedFirstRecordFrom(results)

    Assert.assertEquals("""{ "inner" : 5}""", record.getOrElse("inner-parent", ""))
  }

  @Test
  def testDbGetQueryWithKeys{
    val rMongo = new RMongo("test")
    val results = rMongo.dbGetQuery("test_data", """ {} """, """ {"foo": 1} """, 0, 100)
    val record = parsedFirstRecordFrom(results)

    Assert.assertEquals("bar", record.getOrElse("foo", ""))
    Assert.assertEquals("", record.getOrElse("size", ""))
  }

  @Test
  def testDbGetQueryRegex{
    val rMongo = new RMongo("test")
    val results = rMongo.dbGetQuery("test_data", """ {"foo": {"$regex": "bar", "$options": "i"}} """)
    val record = parsedFirstRecordFrom(results)

    Assert.assertEquals("bar", record.getOrElse("foo", ""))
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

    Assert.assertEquals("n1", record.getOrElse("foo", ""))
  }

  @Test
  def testDbGetQueryPaginate{
    val rMongo = new RMongo("test")
    val page1 = rMongo.dbGetQuery("test_data", """ {} """, 0, 1)
    val record1 = parsedFirstRecordFrom(page1)

    Assert.assertEquals("bar", record1.getOrElse("foo", ""))

    val page2 = rMongo.dbGetQuery("test_data", """ {} """, 1, 1)
    val record2 = parsedFirstRecordFrom(page2)

    Assert.assertEquals("n1", record2.getOrElse("foo", ""))
  }

  @Test
  def testToCsvOutput{
    clearTestDB

    val rMongo = new RMongo("test")

    val doc = """ {"_id": "foo", "foo": "bar\n\r should not break", "size": 5} """
    val response = rMongo.dbInsertDocument("test_data", doc)

    val query = "{}"
    val queryObject = JSON.parse(query).asInstanceOf[DBObject]
    val collection = db.getCollection("test_data")
    val cursor = collection.find(queryObject)

    val results = RMongo.toCsvOutput(cursor)

    assert(results != null)
  }

  def parsedFirstRecordFrom(results: String):Map[String, Any] = {
    val lines = results.split("\n").filter(_.size > 0)

    val keys = lines.head.split(RMongo.SEPARATOR )
    val entries = lines.drop(1)
    val entry = entries.headOption.getOrElse("").split(RMongo.SEPARATOR )

    keys.zip(entry).toMap
  }

  @Test
  def testDbGetDistinct{
     val rMongo = new RMongo("test")
     val results = rMongo.dbGetDistinct("test_data", "size")

     Assert.assertEquals("\"5\"\n\"10\"", results)
  }

  @Test
  def testDbAggregate{
     val rMongo = new RMongo("test")
     var pipeline = Array(
       """ { "$project" : { "baz" : "$foo" } } """,
       """ { "$group" : { "_id" : "$baz" } } """,
       """ { "$match" : { "_id" : "bar" } } """)
     val results = rMongo.dbAggregate("test_data", pipeline)

     //Assert.assertEquals("\"{ \"_id\" : \"bar\"}\"", results)
     assert(results.contains("{ \"_id\" : \"bar\"}"))
  }
}
