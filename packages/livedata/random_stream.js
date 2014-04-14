// RandomStream allows for generation of pseudo-random values, from a seed.
//
// We use this for consistent 'random' numbers across the client and server.
// We want to generate probably-unique IDs on the client, and we ideally want
// the server to generate the same IDs when it executes the method.
//
// For generated values to be the same, we must seed ourselves the same way,
// and we must keep track of the current state of our pseudo-random generators.
// We call this state the scope. By default, we use the current DDP method
// invocation as our scope.  DDP now allows the client to specify a randomSeed.
// If a randomSeed is provided it will be used to seed our random sequences.
// In this way, client and server method calls will generate the same values.
//
// We expose multiple streams, each keyed by a string;
// each stream is independent and seeded differently (but predictably).
// By using multiple streams, we support reordering of requests,
// as long as they occur on different streams.
RandomStream = function (options) {
  var self = this;

  this.seed = [].concat(options.seed || randomToken());

  this._sequences = {};
};

// Returns a random string of sufficient length for a random seed.
// This is a placeholder function; a similar function is planned
// for Random itself; when that is added we should remove this function,
// and call Random's randomToken instead.
function randomToken() {
  return Random.hexString(20);
};

// Returns the random stream with the specified key, in the specified scope.
// If scope is null (or otherwise falsey) then we will use Random, which will
// give us as random numbers as possible, but won't produce the same
// values across client and server.
// However, scope will normally be the current DDP method invocation, so
// we'll use the stream with named key, and we should get consistent
// values on the client and server sides of a method call.
RandomStream.get = function (scope, key) {
  if (!key) {
    key = "default";
  }
  if (!scope) {
    // There was no scope passed in;
    // the sequence won't actually be reproducible.
    return Random;
  }
  var randomStream = scope.randomStream;
  if (!randomStream) {
    scope.randomStream = randomStream = new RandomStream({
      seed: scope.randomSeed
    });
  }
  return randomStream._sequence(key);
};

// Returns the named sequence of pseudo-random values.
// The scope will be DDP._CurrentInvocation.get(), so the stream will produce
// consistent values for method calls on the client and server.
DDP.randomStream = function (name) {
  var scope = DDP._CurrentInvocation.get();
  return RandomStream.get(scope, name);
};

// Creates a randomSeed for passing to a method call.
// Note that we take enclosing as an argument,
// though we expect it to be DDP._CurrentInvocation.get()
// However, we often evaluate makeRpcSeed lazily, and thus the relevant
// invocation may not be the one currently in scope.
// If enclosing is null, we'll use Random and values won't be repeatable.
makeRpcSeed = function (enclosing, methodName) {
  var stream = RandomStream.get(enclosing, '/rpc/' + methodName);
  return stream.hexString(20);
};

_.extend(RandomStream.prototype, {
  // Get a random sequence with the specified key, creating it if does not exist.
  // New sequences are seeded with the seed concatenated with the key.
  // By passing a seed into Random.create, we use the Alea generator.
  _sequence: function (key) {
    var self = this;

    var sequence = self._sequences[key] || null;
    if (sequence === null) {
      var sequenceSeed = self.seed.concat(key);
      for (var i = 0; i < sequenceSeed.length; i++) {
        if (_.isFunction(sequenceSeed[i])) {
          sequenceSeed[i] = sequenceSeed[i]();
        }
      }
      self._sequences[key] = sequence = Random.createWithSeeds.apply(null, sequenceSeed);
    }
    return sequence;
  }
});
