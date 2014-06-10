var Fiber = Npm.require('fibers');
var handlesForRun = {};
var reportsForRun = {};

Meteor.publish(Meteor._ServerTestResultsSubscription, function (runId) {
  check(runId, String);
  var self = this;
  if (!_.has(handlesForRun, runId))
    handlesForRun[runId] = [self];
  else
    handlesForRun[runId].push(self);
  self.onStop(function () {
    handlesForRun[runId] = _.without(handlesForRun[runId], self);
  });
  if (_.has(reportsForRun, runId)) {
    self.added(Meteor._ServerTestResultsCollection, runId,
               reportsForRun[runId]);
  } else {
    self.added(Meteor._ServerTestResultsCollection, runId, {});
  }
  self.ready();
});

Meteor.methods({
  'tinytest/run': function (runId, pathPrefix) {
    check(runId, String);
    check(pathPrefix, Match.Optional([String]));
    this.unblock();

    reportsForRun[runId] = {};

    var addDatum = function (datum) {
      var dummyKey = Random.id();
      var fields = {};
      fields[dummyKey] = datum;
      _.each(handlesForRun[runId], function (handle) {
        handle.changed(Meteor._ServerTestResultsCollection, runId, fields);
      });
      // Save for future subscriptions.
      reportsForRun[runId][dummyKey] = report;
    };

    var onReport = function (report) {
      if (! Fiber.current) {
        Meteor._debug("Trying to report a test not in a fiber! "+
                      "You probably forgot to wrap a callback in bindEnvironment.");
        console.trace();
      }
      addDatum(report);
    };

    var onComplete = function() {
      if (! Fiber.current) {
        Meteor._debug("Trying to report a test not in a fiber! "+
                      "You probably forgot to wrap a callback in bindEnvironment.");
        console.trace();
      }
      var datum = { id: runId, done: true };
      addDatum(datum);
    };

    Tinytest._runTests(onReport, onComplete, pathPrefix);
  },
  'tinytest/clearResults': function (runId) {
    check(runId, String);
    _.each(handlesForRun[runId], function (handle) {
      // XXX this doesn't actually notify the client that it has been
      // unsubscribed.
      handle.stop();
    });
    delete handlesForRun[runId];
    delete reportsForRun[runId];
  }
});
