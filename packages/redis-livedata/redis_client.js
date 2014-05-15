/**
 * Simple wrapper/helpers for the Redis NPM client.  Server only.
 */

var RedisNpm = Npm.require('redis');

RedisClient = function (url, options) {
  var self = this;
  options = options || {};

  // TODO: Parse url into host/port
  var port = 6379;
  var host = "127.0.0.1";
  var options = {};
  self._client = RedisNpm.createClient(port, host, options);
};

RedisClient.prototype.subscribeKeyspaceEvents = function (callback, listener) {
  var self = this;

  self._client.on("pmessage", function (pattern, channel, message) {
    Meteor._debug("Redis ("+  pattern +")" + " notification: " + channel + ": " + message);
    var colonIndex = channel.indexOf(":");
    if (channel.indexOf("__keyspace@") != 0 || colonIndex == 0) {
      Meteor._debug("Unrecognized channel: " + channel);
      return;
    }
    var key = channel.substr(colonIndex+1);
    listener(key, message);
  });
  self._client.psubscribe("__keyspace@*", callback);
};

RedisClient.prototype.findCandidateKeys = function (collectionName, matcher, callback) {
  var self = this;

  // Special case the single-document matcher
  // {"_paths":{"_id":true},"_hasGeoQuery":false,"_hasWhere":false,"_isSimple":true,"_selector":{"_id":"XhjyfgEbYyoYTiABX"}}
  var simpleKeys = null;
  if (!matcher._hasGeoQuery && !matcher._hasWhere && matcher._isSimple) {
    var keys = _.keys(matcher._selector);
    Meteor._debug("keys: " + keys);
    if (keys.length == 1 && keys[0] === "_id") {
      var selectorId = matcher._selector._id;
      if (typeof selectorId === 'string') {
        simpleKeys = [collectionName + "//" + selectorId];
        Meteor._debug("Detected simple id query: " + simpleKeys);
      }
    }
  }
  
  if (simpleKeys === null) {
    self._client.keys(collectionName + "//*", Meteor.bindEnvironment(callback));
  } else {
    callback(null, simpleKeys);
  }
};



RedisClient.prototype.incr = function (key, callback) {
  var self = this;

  self._client.incr(key, callback);
};

RedisClient.prototype.getAll = function (keys, callback) {
  var self = this;
  
  var client = self._client;

  var errors = [];
  var values = [];
  var replyCount = 0;
  
  var n = keys.length;
  
  if (n == 0) {
    callback(errors, values);
    return;
  }

  _.each(_.range(n), function(i) {
    var key = keys[i];
    client.get(key, Meteor.bindEnvironment(function(err, value) {
      if (err) {
        Meteor._debug("Error getting key from redis: " + err);
      }
      errors[i] = err;
      values[i] = value;
      
      replyCount++;
      if (replyCount == n) {
        callback(errors, values);
      }
    }));
  });
};

RedisClient.prototype.setAll = function (keys, values, callback) {
  var self = this;
  
  var client = self._client;

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
    var value = values[i];
    
    client.set(key, value, Meteor.bindEnvironment(function(err, result) {
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


RedisClient.prototype.removeAll = function (keys, callback) {
  var self = this;
  
  var client = self._client;

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
    client.del(key, Meteor.bindEnvironment(function(err, result) {
      if (err) {
        Meteor._debug("Error deleting key in redis: " + err);
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
