/**
 * Simple wrapper/helpers for the Redis NPM client. Server only.
 */

var NpmMysql = Npm.require('mysql');

SqlClient = function(url, options) {
  var self = this;
  options = options || {};

  // TODO: Parse url into host/port
  var options = {
    host : '127.0.0.1',
    user : 'meteor',
    password : 'secret',
    database : 'meteor'
  };
  self._connection = NpmMysql.createConnection(options);
};

// RedisClient.prototype.subscribeKeyspaceEvents = function (callback, listener)
// {
// var self = this;
//
// self._client.on("pmessage", function (pattern, channel, message) {
// Meteor._debug("Redis ("+ pattern +")" + " notification: " + channel + ": " +
// message);
// var colonIndex = channel.indexOf(":");
// if (channel.indexOf("__keyspace@") != 0 || colonIndex == 0) {
// Meteor._debug("Unrecognized channel: " + channel);
// return;
// }
// var key = channel.substr(colonIndex+1);
// listener(key, message);
// });
// self._client.psubscribe("__keyspace@*", callback);
// };

function getSimpleKeys(matcher) {
  // Special case the single-document matcher
  // {"_paths":{"_id":true},"_hasGeoQuery":false,"_hasWhere":false,"_isSimple":true,"_selector":{"_id":"XhjyfgEbYyoYTiABX"}}
  var simpleKeys = null;
  if (!matcher._hasGeoQuery && !matcher._hasWhere && matcher._isSimple) {
    var keys = _.keys(matcher._selector);
    Meteor._debug("keys: " + keys);
    if (keys.length == 1 && keys[0] === "_id") {
      var selectorId = matcher._selector._id;
      if (typeof selectorId === 'string') {
        simpleKeys = [ selectorId ];
        Meteor._debug("Detected simple id query: " + simpleKeys);
      }
    }
  }
  return simpleKeys;
}

SqlClient.prototype.getMetadata = function(tableName, callback) {
  var self = this;

  var connection = self._connection;

  var sql = "SHOW COLUMNS FROM ??";
  var params = [ tableName ];

  connection.query(sql, params, function(err, results) {
    if (err) {
      callback(err, null);
      return;
    }

    var columns = results;
    var sqlColumnNames = [];

    var columnMap = {};
    var jsonColumn = null;

    for (var i = 0; i < columns.length; i++) {
      var column = columns[i];
      var key = column.Field;
      sqlColumnNames.push(NpmMysql.escapeId(key));
      if (key == 'json') {
        jsonColumn = i;
        continue;
      } else {
        columnMap[key] = i;
      }
    }

    var metadata = {};
    metadata.jsonColumn = jsonColumn;
    metadata.columnMap = columnMap;
    metadata.columns = columns;
    metadata.sqlColumnNames = sqlColumnNames;
    metadata.tableName = tableName;

    callback(null, metadata);
  });
};

SqlClient.prototype.getAll = function(metadata, matcher, sorter, skip, limit,
    projection, callback) {
  var self = this;

  var tableName = metadata.tableName;
  var connection = self._connection;

  var simpleKeys = getSimpleKeys(matcher);
  var sql = "SELECT * FROM ??";
  var params = [ tableName ];
  if (simpleKeys !== null) {
    if (simpleKeys.length == 1) {
      sql = sql + " WHERE id=??";
      params.push(simpleKeys[0]);
    } else {
      assert(false);
    }
  }

  var processRows = function(err, rows) {
    var results = [];
    Meteor._debug("Process rows: " + err + "," + rows);
    if (err) {
      if (err) {
        Meteor._debug("Error listing rows in SQL: " + err);
      }
      callback(err, null);
      return;
    }

    for (var i = 0; i < rows.length; i++) {
      var row = rows[i];
      var doc = row;
      if (metadata.jsonColumn !== null) {
        var json = row['json'];
        delete doc['json'];
        if (json != "") {
          //Meteor._debug("JSON: " + json);
          doc = _.extend(doc, JSON.parse(json));
        }
      }

      Meteor._debug(i + ": " + JSON.stringify(doc));

      if (matcher.documentMatches(doc).result) {
        results.push(doc);
      }
    }

    if (sorter) {
      var comparator = sorter.getComparator();
      results.sort(comparator);
    }
    if (skip) {
      results = results.slice(skip);
    }
    if (limit) {
      results = results.slice(0, limit);
    }

    // TODO: Before sort / skip / limit?
    if (projection) {
      for (var i = 0; i < results.length; i++) {
        var o = results[i];
        o = projection(o);
        results[i] = o;
      }
    }

    callback(null, results);
  };

  connection.query(sql, params, processRows);
};

SqlClient.prototype.setAll = function(keys, values, callback) {
  throw Exception("NOT YET SUPPORTED");
  // var self = this;
  //  
  // var client = self._client;
  //
  // var errors = [];
  // var results = [];
  //  
  // var n = keys.length;
  // if (n == 0) {
  // callback(errors, results);
  // return;
  // }
  //
  // var replyCount = 0;
  // _.each(_.range(n), function(i) {
  // var key = keys[i];
  // var value = values[i];
  //    
  // client.set(key, value, Meteor.bindEnvironment(function(err, result) {
  // if (err) {
  // Meteor._debug("Error setting value in redis: " + err);
  // }
  // errors[i] = err;
  // results[i] = result;
  //      
  // replyCount++;
  // if (replyCount == n) {
  // callback(errors, results);
  // }
  // }));
  // });
};

SqlClient.prototype.insertAll = function(tableName, metadata, docs, options,
    callback) {

  var self = this;
  var connection = self._connection;

  Meteor._debug("SqlClient.insertAll(" + JSON.stringify(arguments) + ")");

  var rows = [];

  var columnMap = metadata.columnMap;

  for (var i = 0; i < docs.length; i++) {
    var doc = docs[i];

    var id = doc._id;
    if (id === undefined) {
      doc._id = id = Random.hexString(24);
    }

    var row = [];
    var json = null;
    _.each(_.keys(doc), function(key) {
      var columnIndex = columnMap[key];
      var v = doc[key];
      if (columnIndex !== undefined) {
        row[columnIndex] = v;
      } else {
        if (json === null) {
          json = {};
        }
        json[key] = v;
      }
    });

    if (json !== null) {
      row[metadata.jsonColumn] = JSON.stringify(json);
    }

    rows.push(row);
  }

  var sql = "INSERT INTO ?? (" + metadata.sqlColumnNames.join(",")
      + ") VALUES ?";
  var params = [ tableName ];
  params.push(rows);
  connection.query(sql, params, Meteor.bindEnvironment(callback));
};

SqlClient.prototype.updateAll = function(tableName, selector, mod, options,
    callback) {
  throw Exception("NOT YET SUPPORTED");

  // var collectionName = self.collectionName;
  // var extra = {};
  //  
  // var upsert = !!options.upsert;
  //  
  // var matcher = new Minimongo.Matcher(selector, self);
  //
  // var client = self.connection.db;
  //
  // client.findMatchingRows(self.collectionName, matcher, function (err, keys)
  // {
  // if (err) {
  // if (err) {
  // Meteor._debug("Error listing keys in SQL: " + err);
  // }
  // deferredCallback(err, null, extra);
  // return;
  // }
  //
  // Meteor._debug("Fetching keys: " + JSON.stringify(keys));
  //
  // client.getAll(keys, function(errors, values) {
  // var setKeys = [];
  // var setValues = [];
  //      
  // for (var i = 0; i < values.length; i++) {
  // var value = values[i];
  // if (value === null || value === undefined) {
  // Meteor._debug("Error reading key (concurrent modification?): " + keys[i]);
  // continue;
  // }
  // var doc = JSON.parse(value);
  // if (!matcher.documentMatches(doc).result) {
  // continue;
  // }
  //        
  // LocalCollection._modify(doc, mod, options);
  //
  // // TODO: Validate id hasn't changed
  // var key = keys[i];
  // var newValue = JSON.stringify(doc);
  //        
  // Meteor._debug("Updating " + keys[i] + ": " + value + " => " + newValue);
  //
  // setKeys.push(key);
  // setValues.push(newValue);
  // }
  //      
  // if (setKeys.length) {
  // Meteor._debug("set: " + JSON.stringify(setKeys) + " => " +
  // JSON.stringify(setValues));
  // client.setAll(setKeys, setValues, function (errors, results) {
  // for (var i = 0; i < errors.length; i++) {
  // if (errors[i] === null || errors[i] === undefined) {
  // continue;
  // }
  // var err = "Error setting value in SQL";
  // deferredCallback(err, null, extra);
  // return;
  // }
  // deferredCallback(null, setValues.length, extra);
  // });
  // } else if (upsert) {
  // Meteor._debug("Upsert with no update; doing insert");
  //          
  // var newDoc;
  // try {
  // newDoc = LocalCollection._removeDollarOperators(selector);
  //            
  // // Not sure why this is needed here, but not in minimongo's equivalent code
  // if (mod._id && !newDoc._id) {
  // newDoc._id = mod._id;
  // }
  // LocalCollection._modify(newDoc, mod, {isInsert: true});
  // } catch (e) {
  // var err = {};
  // err.code = 0;
  // err.err = e.message;
  //          
  // Meteor._debug("selector: " + JSON.stringify(selector));
  // Meteor._debug("mod: " + JSON.stringify(mod));
  //          
  // Meteor._debug("Caught exception in LocalCollection._modify: " +
  // JSON.stringify(err));
  // deferredCallback(err, 0, extra);
  // return;
  // }
  // if (! newDoc._id && options.insertedId) {
  // newDoc._id = options.insertedId;
  // }
  //          
  // Meteor._debug("Doing insert for upsert: " + JSON.stringify(newDoc));
  // var insertCallback = function (err, results) {
  // if (err) {
  // deferredCallback(err, 0, extra);
  // } else {
  // deferredCallback(null, 1, extra);
  // }
  // };
  // var docs = [newDoc];
  // self.insert(collectionName, docs, options, insertCallback);
  // }
  // });
  // });
};

SqlClient.prototype.deleteWhere = function(tableName, matcher, callback) {
  throw Exception("NOT YET SUPPORTED");
  //  
  // var self = this;
  //  
  // var connection = self._connection;
  //
  // var simpleKeys = getSimpleKeys(matcher);
  // var sql = "SELECT * FROM ??";
  // var params = [ tableName ];
  // if (simpleKeys !== null) {
  // if (simpleKeys.length == 1) {
  // sql = "DELETE FROM ?? WHERE id=??";
  // params.push(tableName, simpleKeys[0]);
  // } else {
  // assert(false);
  // }
  // }
  //  
  // var processRows = function (err, rows) {
  // if (err) {
  // if (err) {
  // Meteor._debug("Error listing rows in SQL: " + err);
  // }
  // callback(err, null);
  // return;
  // }
  //
  // for (var i = 0; i < rows.length; i++) {
  // var row = rows[i];
  // var doc = JSON.parse(value);
  //
  // Meteor._debug(keys[i] + ": " + value + " => " + JSON.stringify(doc));
  //
  // if (matcher.documentMatches(doc).result) {
  // results.push(doc);
  // }
  // }
  //      
  // if (sorter) {
  // var comparator = sorter.getComparator();
  // results.sort(comparator);
  // }
  // if (skip) {
  // results = results.slice(skip);
  // }
  // if (limit) {
  // results = results.slice(0, limit);
  // }
  //      
  // // TODO: Before sort / skip / limit?
  // if (projection) {
  // for (var i = 0; i < results.length; i++) {
  // var o = results[i];
  // o = projection(o);
  // results[i] = o;
  // }
  // }
  //      
  // callback(null, results);
  // };
  //  
  // connection.query(sql, params, processRows);
  //  
  //  
  // var self = this;
  //  
  // var connection = self._connection;
  //  
  // // TODO: MySQL escaping in the driver is handy, but also terrifying
  // connection.query('DELETE FROM ?? WHERE id IN (??)', [ tableName, ids ],
  // function (err, results) {
  // if (err) {
  // Meteor._debug("Error deleting rows in SQL: " + err);
  // }
  // callback(err, results);
  // });
  //  
  //
  // client.deleteWhere(tableName, matcher, function (err, count) {
  // if (err) {
  // if (err) {
  // Meteor._debug("Error listing keys in SQL: " + err);
  // }
  // deferredCallback(err, null, extra);
  // return;
  // }
  //
  // Meteor._debug("Deleting keys: " + JSON.stringify(keys));
  //
  // client.removeAll(tableName, keys, function(err, results) {
  // deferredCallback(err, results.length);
  // });
  // });
};
