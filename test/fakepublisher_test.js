suite("Exchanges (FakePublisher)", function() {
  var assert     = require('assert');
  var subject    = require('../');
  var config     = require('typed-env-config');
  var validator  = require('taskcluster-lib-validate');
  var path       = require('path');
  var debug      = require('debug')('test');
  var _          = require('lodash');

  var exchanges = null;
  setup(async function() {
    exchanges = new subject({
      title:              "Title for my Events",
      description:        "Test exchanges used for testing things only"
    });
    // Check that we can declare an exchange
    exchanges.declare({
      exchange:           'test-exchange',
      name:               'testExchange',
      title:              "Test Exchange",
      description:        "Place we post message for **testing**.",
      routingKey: [
        {
          name:           'testId',
          summary:        "Identifier that we use for testing",
          multipleWords:  false,
          required:       true,
          maxSize:        22
        }, {
          name:           'taskRoutingKey',
          summary:        "Test specific routing-key: `test.key`",
          multipleWords:  true,
          required:       true,
          maxSize:        128
        }, {
          name:           'state',
          summary:        "State of something",
          multipleWords:  false,
          required:       false,
          maxSize:        16
        }, {
          name:           'index',
          summary:        "index of something",
          multipleWords:  false,
          required:       false,
          maxSize:        3
        }, {
          name:           'myConstant',
          summary:        "Some constant to test",
          constant:       "-constant-"
        }
      ],
      schema:             'http://localhost:1203/exchange-test-schema.json#',
      messageBuilder:     function(msg) { return msg; },
      routingKeyBuilder:  function(msg, rk) { return rk; },
      CCBuilder:          function(msg, rk, cc = []) {return cc;}
    });

    var validate = await validator({
      folder:  path.join(__dirname, 'schemas'),
      baseUrl: 'http://localhost:1203/'
    });

    // Set options on exchanges
    exchanges.configure({
      validator:              validate,
      credentials:            {fake: true}, // indicate use of FakePublisher
    });
  });

  // Test that we can connect to AMQP server
  test("connect", function() {
    return exchanges.connect().then(function(publisher) {
      assert(publisher instanceof subject.FakePublisher,
             "Should get an instance of exchanges.Publisher");
    });
  });

  // Test that we can publish messages
  test("publish message", function() {
    var published = []
    return exchanges.connect().then(function(publisher) {
      publisher.on('fakePublish', function(info) { published.push(info); });
      return publisher.testExchange({someString: "My message"}, {
        testId:           "myid",
        taskRoutingKey:   "some.string.with.dots",
        state:            undefined // Optional
      });
    }).then(function() {
      assert(_.isEqual(published, [{
        exchange: 'test-exchange',
        routingKey: 'myid.some.string.with.dots._._.-constant-',
        payload: { someString: 'My message' },
        CCs: [] }]));
    });;
  });

  // most of the validation s handled in testing the real publisher; this just checks that
  // the same validation is occurring for the fake

  // Test publication fails on schema violation
  test("publish error w. schema violation", function() {
    return exchanges.connect().then(function(publisher) {
      return publisher.testExchange({
        someString:   "My message",
        "volation":   true
      }, {
        testId:           "myid",
        taskRoutingKey:   "some.string.with.dots",
        state:            undefined // Optional
      });
    }).then(function() {
      assert(false, "Expected an error");
    }, function(err) {
      // Expected an error
      debug("Got expected Error: %s, %j", err, err);
    });
  });


  // Test publication fails on required key missing
  test("publish error w. required key missing", function() {
    return exchanges.connect().then(function(publisher) {
      return publisher.testExchange({
        someString:   "My message",
      }, {
        taskRoutingKey:   "some.string.with.dots",
        state:            "here"
      });
    }).then(function() {
      assert(false, "Expected an error");
    }, function(err) {
      // Expected an error
      debug("Got expected Error: %s, %j", err, err);
    });
  });
});

