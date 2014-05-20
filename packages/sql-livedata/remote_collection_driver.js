SqlInternals.RemoteCollectionDriver = function (
  mongo_url, options) {
  var self = this;
  self.mongo = new SqlConnection(mongo_url, options);
};

_.extend(SqlInternals.RemoteCollectionDriver.prototype, {
  open: function (name) {
    var self = this;
    var ret = {};
    _.each(
      ['find', 'findOne', 'insert', 'update', , 'upsert',
       'remove', '_ensureIndex', '_dropIndex', '_createCappedCollection',
       'dropCollection'],
      function (m) {
        ret[m] = _.bind(self.mongo[m], self.mongo, name);
      });
    return ret;
  }
});


// Create the singleton RemoteCollectionDriver only on demand, so we
// only require Mongo configuration if it's actually used (eg, not if
// you're only trying to receive data from a remote DDP server.)
SqlInternals.defaultRemoteCollectionDriver = _.once(function () {
  var mongoUrl;
  var connectionOptions = {};

  AppConfig.configurePackage("redis-livedata", function (config) {
    // This will keep running if mongo gets reconfigured.  That's not ideal, but
    // should be ok for now.
    mongoUrl = config.url;

    if (config.oplog)
      connectionOptions.oplogUrl = config.oplog;
  });

  if (!mongoUrl) {
	  mongoUrl = "127.0.0.1";
  }

  // XXX bad error since it could also be set directly in METEOR_DEPLOY_CONFIG
  if (!mongoUrl)
    throw new Error("MONGO_URL must be set in environment");


  return new SqlInternals.RemoteCollectionDriver(mongoUrl, connectionOptions);
});
