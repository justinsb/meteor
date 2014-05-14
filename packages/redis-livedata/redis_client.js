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


RedisClient.prototype.incr = function (key, callback) {
  var self = this;

  self._client.incr(key, callback);
};

