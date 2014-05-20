Tinytest.add(
  'collection - call Meteor.SqlCollection without new',
  function (test) {
    test.throws(
      function () {
        Meteor.SqlCollection(null);
      },
      /use "new" to construct a Meteor\.SqlCollection/
    );
  }
);
