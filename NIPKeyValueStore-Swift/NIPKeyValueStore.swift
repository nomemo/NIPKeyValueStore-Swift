import Foundation

func debugLog(input : AnyObject) {
  println("debugLog:\(input)")
}


func PATH_OF_DOCUMENT () -> NSString {
  return NSSearchPathForDirectoriesInDomains(.DocumentDirectory, .UserDomainMask, true)[0] as NSString
}


class NIPKeyValueItem : NSObject{
  var itemId: String
  var itemObject : AnyObject?
  var createdTime : NSDate
  
  override init() {
    self.itemId = "0"
    self.createdTime = NSDate()
  }
  
  init (itemId :String, itemObject :AnyObject, createdTime :NSDate ) {
    self.itemId = itemId
    self.itemObject = itemObject
    self.createdTime = createdTime
  }
  
  func description ( () -> String ) {
    "id=\(itemId), value=\(itemObject), timeStamp=\(createdTime)"
  }
}

let DEFAULT_DB_NAME = "database.sqlite"

func CREATE_TABLE_SQL ( tableName :String ) -> String! {
  return "CREATE TABLE IF NOT EXISTS \(tableName) (" +
    "id TEXT NOT NULL," +
    "json TEXT NOT NULL," +
    "createdTime TEXT NOT NULL," +
  "PRIMARY KEY(id)) "
}

func UPDATE_ITEM_SQL ( tableName :String ) -> String! {
  return "REPLACE INTO \(tableName) (id, json, createdTime) values (?, ?, ?)"
}

func QUERY_ITEM_SQL ( tableName :String) -> String! {
  return "SELECT * from \(tableName) where id = ? Limit 1"
}

func SELECT_ALL_SQL ( tableName :String) -> String! {
  return "SELECT * from \(tableName)"
}

func CLEAR_ALL_SQL ( tableName :String) -> String! {
  return "DELETE from \(tableName)"
}

func DELETE_ITEM_SQL ( tableName :String) -> String! {
  return "DELETE from \(tableName) where id = ?"
}

func DELETE_ITEMS_SQL ( tableName :String, idRange :String) -> String! {
  return "DELETE from \(tableName) where id in ( \(idRange) )"
}

func DELETE_ITEMS_WITH_PREFIX_SQL ( tableName :String) -> String! {
  return "DELETE from \(tableName) where id like ? "
}


var DBColumnJson = "json"
var DBColumnCreateAt = "createdTime"
var DBColumnId = "id"

func BuildNIPItemByRS (rs:FMResultSet!) -> NIPKeyValueItem? {
  var json = rs.stringForColumn(DBColumnJson)
  if json == nil {
    return nil
  }
  var createTime = rs.dateForColumn(DBColumnCreateAt)
  var objectId = rs.stringForColumn(DBColumnId)
  var error:NSError?
  var jsonData = json.dataUsingEncoding(NSUTF8StringEncoding, allowLossyConversion: false)
  var decodeResult: AnyObject? = NSJSONSerialization.JSONObjectWithData(jsonData! , options: NSJSONReadingOptions.AllowFragments, error: &error)
  if error != nil {
    debugLog(error!)
    return nil
  }
  var item = NIPKeyValueItem(itemId: objectId, itemObject: decodeResult!, createdTime: createTime)
  return item
}

class NIPKeyValueStore : NSObject{
  var dbQueue : FMDatabaseQueue?
  
  class func checkTableName( tableName: String? ) -> Bool {
    if (tableName?.rangeOfString(" ", options: NSStringCompareOptions.CaseInsensitiveSearch , range: nil, locale: nil) == nil)  {
      if let tb = tableName {
        return true
      }
    }
    return false
  }
  
  deinit {
    dbQueue?.close()
  }
  
  init(name : String) {
    var dbPath = (PATH_OF_DOCUMENT() as NSString).stringByAppendingPathComponent(name)
    debugLog("db path is \(dbPath)")
    dbQueue?.close()
    dbQueue = FMDatabaseQueue(path: dbPath)
  }
  
  init(path : String) {
    debugLog("db path is \(path)")
    if ((dbQueue) != nil) {
      dbQueue?.close()
    }
    dbQueue = FMDatabaseQueue(path: path)
  }
    
  func createTable(tableName: String) {
    if (!NIPKeyValueStore.checkTableName(tableName)) {
      return 
    }
    var CreateSQL = CREATE_TABLE_SQL(tableName)
    dbQueue?.inDatabase({ (db:FMDatabase!) -> Void in
      var result =  db.executeStatements(CreateSQL)
      if (result) {
        
      }
      return 
    })
  }
  
  func clearTable( tableName: String) {
    if (!NIPKeyValueStore.checkTableName(tableName)) {
      return 
    }
    var ClearSQL = CLEAR_ALL_SQL(tableName)
    dbQueue?.inDatabase({ (db:FMDatabase!) -> Void in
      var result =  db.executeStatements(ClearSQL)
      if (result) {
        
      }
      return 
    })
  }
  
  
  func putObject(object:AnyObject, objectId:String,fromTable tableName:String) {
    if(!NIPKeyValueStore.checkTableName(tableName)) {
      return
    }
    var error:NSError?
    var data = NSJSONSerialization.dataWithJSONObject(object, options: NSJSONWritingOptions.PrettyPrinted, error: &error)
    if ((error) != nil) {
      debugLog("ERROR, faild to get json data")
      return
    }
    var jsonString:String! = NSString(data: data!, encoding: NSUTF8StringEncoding)
    var createTime = NSDate()
    var updateSQL:String! = UPDATE_ITEM_SQL(tableName)
    dbQueue?.inDatabase({ (db:FMDatabase!) -> Void in
      var inputArray:[AnyObject] = []
      inputArray.append(objectId)
      inputArray.append(jsonString)
      inputArray.append(createTime)
      var result = db.executeUpdate(updateSQL, withArgumentsInArray: inputArray)
      if (!result) {
        debugLog("ERROR, failed to insert/replace into table: \(tableName)")
      }
    })
  }
  
  func getObjectById( objectID:String,fromTable tableName:String)->AnyObject? {
    var item = self.getNIPKeyValueItem(objectID, fromTable: tableName)
    return item?.itemObject
  }
  
  func getNIPKeyValueItem( objectId:String,fromTable tableName:String) -> NIPKeyValueItem? {
    if (!NIPKeyValueStore.checkTableName(tableName)) {
      return nil
    }
    var QuerySQL = QUERY_ITEM_SQL(tableName)
    var json:AnyObject?
    var createdTime:NSDate?
    var result:NIPKeyValueItem? = nil
    dbQueue?.inDatabase({ (db:FMDatabase!) -> Void in
      var rs:FMResultSet? = db.executeQuery(QuerySQL, withArgumentsInArray: [objectId])
      if ((rs?.next()) != nil) {
        result = BuildNIPItemByRS(rs)
      }
      rs?.close()
    })
    
    return result
  }
  
  func put(string:String, stringID:String,fromTable tableName:String) {
    if (!NIPKeyValueStore.checkTableName(tableName)) {
      return 
    }
    self.putObject(string, objectId: stringID, fromTable: tableName)
  }
  
  func getString(stringID:String,fromTable tableName:String) -> String? {
    if (!NIPKeyValueStore.checkTableName(tableName)) {
      return nil
    }
    var result: AnyObject? = self.getObjectById(stringID, fromTable: tableName)
    if let stringResult = result as? String {
      return stringResult
    }
    return nil
  }
  
  func put(number:NSNumber, numberID:String,fromTable tableName:String) {
    if (!NIPKeyValueStore.checkTableName(tableName)) {
      return
    }
    self.putObject(number, objectId: numberID, fromTable: tableName)
  }
  
  func getNumber(numberID:String,fromTable tableName:String) -> NSNumber? {
    if (!NIPKeyValueStore.checkTableName(tableName)) {
      return nil
    }
    var result: AnyObject? = self.getObjectById(numberID, fromTable: tableName)
    if let stringResult = result as? NSNumber {
      return stringResult
    }
    return nil
  }
  
  func getAllItemFromTable(tableName:String) -> [AnyObject]? {
    if (!NIPKeyValueStore.checkTableName(tableName)) {
      return nil
    }
    var result:[NIPKeyValueItem] = []
    var selectSQL = SELECT_ALL_SQL(tableName)
    dbQueue?.inDatabase({ (db:FMDatabase!) -> Void in
      var rs = db.executeQuery(selectSQL, withArgumentsInArray: [])
      while rs.next() {
        var item = BuildNIPItemByRS(rs)
        if item != nil {
          result.append(item!)
        }
      }
      rs.close()
    })
    return result
  }
  
  func deleteObjectById(objectID:String,fromTable tableName:String) {
    if (!NIPKeyValueStore.checkTableName(tableName)) {
      return
    }
    var deleteSQL = DELETE_ITEM_SQL(tableName)
    dbQueue?.inDatabase({ (db:FMDatabase!) -> Void in
      var result = db .executeUpdate(deleteSQL, withArgumentsInArray: [objectID])
      if result == false {
        debugLog(result)
      }
      return
    })
  }
  
  func deleteByObjectIDs(idArray:[String],fromTable tableName:String) {
    if (!NIPKeyValueStore.checkTableName(tableName)) {
      return
    }
    var array = idArray as NSArray
    var stringBuilder = array.componentsJoinedByString(",")
    var deleteSQL = DELETE_ITEMS_SQL(tableName, stringBuilder)
    self.dbQueue?.inDatabase({ (db:FMDatabase!) -> Void in
      var result = db.executeUpdate(deleteSQL, withArgumentsInArray: nil)
      if (result != false) {
        debugLog(db.lastError())
      }
      return
    })
  }
  
  func deleteByIDPrefixs(objectIdPrefix:String,fromTable tableName:String) {
    if (!NIPKeyValueStore.checkTableName(tableName)) {
      return 
    }
    var sql = DELETE_ITEMS_WITH_PREFIX_SQL(tableName)
    var prefixArgument = objectIdPrefix + "%%"
    dbQueue?.inDatabase({ (db:FMDatabase!) -> Void in
      var result = db.executeUpdate(sql, withArgumentsInArray: [prefixArgument])
      if (result != false) {
        debugLog(db.lastError())
      }
      return
    })
    
  }
  
  func close() {
    self.dbQueue?.close()
  }
  
}