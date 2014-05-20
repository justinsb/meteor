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

  Meteor._debug("Executing SQL: " + sql + " " + JSON.stringify(params));
  connection.query(sql, params, Meteor.bindEnvironment(function(err, results) {
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
  }));
};

SqlClient.prototype._mapRowToDoc = function(metadata, row) {
  var doc = row;
  if (metadata.jsonColumn !== null) {
    var json = row['json'];
    delete doc['json'];
    if (json != "" && json !== undefined && json !== null) {
      Meteor._debug("JSON: " + json);
      doc = _.extend(doc, JSON.parse(json));
    }
  }
  return doc;
};

SqlClient.prototype._mapDocToRow = function(metadata, doc) {
  var columnMap = metadata.columnMap;
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
  return row;
};

SqlClient.prototype._findMatching = function(metadata, matcher, sorter, skip,
    limit, projection, callback) {
  var self = this;

  var tableName = metadata.tableName;
  var connection = self._connection;

  var simpleKeys = getSimpleKeys(matcher);
  var sql = "SELECT * FROM ??";
  var params = [ tableName ];
  if (simpleKeys !== null) {
    if (simpleKeys.length == 1) {
      sql = sql + " WHERE _id=?";
      params.push(simpleKeys[0]);
    } else {
      assert(false);
    }
  }

  var processRows = function(err, rows) {
    var results = [];
    Meteor._debug("Process rows: " + err + "," + JSON.stringify(rows));
    if (err) {
      if (err) {
        Meteor._debug("Error listing rows in SQL: " + err);
      }
      callback(err, null);
      return;
    }

    for (var i = 0; i < rows.length; i++) {
      var row = rows[i];
      var doc = self._mapRowToDoc(metadata, row);

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

  Meteor._debug("Executing SQL: " + sql + " " + JSON.stringify(params));
  connection.query(sql, params, Meteor.bindEnvironment(processRows));
};

SqlClient.prototype.getAll = function(metadata, matcher, sorter, skip, limit,
    projection, callback) {
  var self = this;

  return self._findMatching(metadata, matcher, sorter, skip, limit, projection,
      callback);
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

  for (var i = 0; i < docs.length; i++) {
    var doc = docs[i];

    var id = doc._id;
    if (id === undefined) {
      doc._id = id = Random.hexString(24);
    }

    var row = self._mapDocToRow(metadata, doc);
    rows.push(row);
  }

  var sql = "INSERT INTO ?? (" + metadata.sqlColumnNames.join(",")
      + ") VALUES ?";
  var params = [ tableName ];
  params.push(rows);

  Meteor._debug("Executing SQL: " + sql + " " + JSON.stringify(params));
  connection.query(sql, params, Meteor.bindEnvironment(callback));
};

SqlClient.prototype._doUpdate = function(metadata, keys, oldRows, newRows,
    callback) {
  var self = this;

  var connection = self._connection;

  var errors = [];
  var results = [];

  var n = keys.length;
  if (n == 0) {
    callback(errors, results);
    return;
  }

  var replyCount = 0;
  _.each(_.range(n), function(i) {
    var key = keys[i];
    var oldRow = oldRows[i];
    var newRow = newRows[i];

    var changes = {};
    for (var j = 0; j < metadata.columns.length; j++) {
      var column = metadata.columns[j];
      var oldV = oldRow[j];
      var newV = newRow[j];
      if (oldV === newV) {
        continue;
      }
      changes[column.Field] = newV;
    }
    var sql = "UPDATE ?? SET ? WHERE _id=?";
    var params = [ metadata.tableName, changes, key ];

    Meteor._debug("Executing SQL: " + sql + " " + JSON.stringify(params));
    connection.query(sql, params, Meteor.bindEnvironment(function(err, result) {
      if (err) {
        Meteor._debug("Error setting value in redis: " + err);
      }
      errors[i] = err;
      results[i] = result;

      replyCount++;
      if (replyCount == n) {
        callback(errors, results);
      }
    }));
  });
};

SqlClient.prototype.updateAll = function(metadata, selector, mod, options,
    callback) {
  var self = this;
  var extra = {};

  var upsert = !!options.upsert;

  var matcher = new Minimongo.Matcher(selector, self);

  // TODO: Support basic update statements?
  // TODO: Optimistic concurrency control?

  var processRows = function(err, oldRows) {
    Meteor._debug("Update rows: " + JSON.stringify(oldRows));

    if (err) {
      if (err) {
        Meteor._debug("Error listing keys in SQL: " + err);
      }
      callback(err, null, extra);
      return;
    }

    var newRows = [];
    var updateKeys = [];

    for (var i = 0; i < oldRows.length; i++) {
      var oldRow = oldRows[i];
      var doc = self._mapRowToDoc(metadata, oldRow);

      LocalCollection._modify(doc, mod, options);

      // TODO: Validate id hasn't changed
      var key = doc._id;
      var newRow = self._mapDocToRow(metadata, doc);

      Meteor._debug("Updating " + key + ": " + oldRow + " => " + newRow);

      updateKeys.push(key);
      newRows.push(newRow);
    }

    if (updateKeys.length) {
      Meteor._debug("update: " + JSON.stringify(updateKeys) + " => "
          + JSON.stringify(newRows));
      self._doUpdate(metadata, updateKeys, oldRows, newRows, function(errors,
          results) {
        for (var i = 0; i < errors.length; i++) {
          if (errors[i] === null || errors[i] === undefined) {
            continue;
          }
          var err = "Error setting value in SQL";
          callback(err, null, extra);
          return;
        }
        callback(null, updateKeys.length, extra);
      });
    } else if (upsert) {
      Meteor._debug("Upsert with no update; doing insert");

      var newDoc;
      try {
        newDoc = LocalCollection._removeDollarOperators(selector);

        // Not sure why this is needed here, but not in minimongo's
        // equivalent code
        if (mod._id && !newDoc._id) {
          newDoc._id = mod._id;
        }
        LocalCollection._modify(newDoc, mod, {
          isInsert : true
        });
      } catch (e) {
        var err = {};
        err.code = 0;
        err.err = e.message;

        Meteor._debug("selector: " + JSON.stringify(selector));
        Meteor._debug("mod: " + JSON.stringify(mod));

        Meteor._debug("Caught exception in LocalCollection._modify: "
            + JSON.stringify(err));
        callback(err, 0, extra);
        return;
      }
      if (!newDoc._id && options.insertedId) {
        newDoc._id = options.insertedId;
      }

      Meteor._debug("Doing insert for upsert: " + JSON.stringify(newDoc));
      var insertCallback = function(err, results) {
        if (err) {
          deferredCallback(err, 0, extra);
        } else {
          deferredCallback(null, 1, extra);
        }
      };
      var docs = [ newDoc ];
      self.insertAll(metadata, docs, options, insertCallback);
    }
  };

  self._findMatching(metadata, matcher, null, null, null, null, processRows);
};

SqlClient.prototype.deleteWhere = function(metadata, matcher, callback) {
  var self = this;

  var connection = self._connection;

  // TODO: Support basic update statements?
  // TODO: Optimistic concurrency control?

  var processRows = function(err, rows) {
    if (err) {
      if (err) {
        Meteor._debug("Error listing keys in SQL: " + err);
      }
      callback(err, null);
      return;
    }

    var deleteKeys = [];

    Meteor._debug("metadata = "  + JSON.stringify(metadata));
    
    for (var i = 0; i < rows.length; i++) {
      var row = rows[i];

      var key = row['_id'];
      if (!key) {
        Meteor._debug("Ignoring row with no key: " + JSON.stringify(row));
        continue;
      }
      deleteKeys.push(key);
    }

    if (deleteKeys.length) {
      Meteor._debug("delete: " + JSON.stringify(deleteKeys));

      var sql = "DELETE FROM ?? WHERE _id IN (?)";
      var params = [ metadata.tableName, deleteKeys ];

      Meteor._debug("Executing SQL: " + sql + " " + JSON.stringify(params));
      connection.query(sql, params, Meteor
          .bindEnvironment(function(err, result) {
            if (err) {
              callback(err, null);
            } else {
              callback(null, deleteKeys.length);
            }
          }));
    } else {
      callback(null, 0);
    }
  };

  self._findMatching(metadata, matcher, null, null, null, null, processRows);
};
