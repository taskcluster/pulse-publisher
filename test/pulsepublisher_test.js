suite("Exchanges (Publish on Pulse)", function() {
  var assert     = require('assert');
  var subject    = require('../');
  var config     = require('typed-env-config');
  var monitoring = require('taskcluster-lib-monitor');
  var validator  = require('taskcluster-lib-validate');
  var path       = require('path');
  var fs         = require('fs');
  var debug      = require('debug')('base:test:publish-pulse');
  var Promise    = require('promise');
  var slugid     = require('slugid');
  var amqplib    = require('amqplib');
  var _          = require('lodash');

  var cfg = config({});

  if (!cfg.pulse.password) {
    throw new Error("Skipping 'pulse publisher', missing config file: user-config.yml");
    return;
  }

  // ConnectionString for use with amqplib only
  var connectionString = [
    'amqps://',         // Ensure that we're using SSL
    cfg.pulse.username,
    ':',
    cfg.pulse.password,
    '@',
    cfg.pulse.hostname || 'pulse.mozilla.org',
    ':',
    5671                // Port for SSL
  ].join('');

  var monitor = null;
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
      CCBuilder:          function() {return ["something.cced"];}
    });

    var validate = await validator({
      folder:  path.join(__dirname, 'schemas'),
      baseUrl: 'http://localhost:1203/'
    });

    monitor = await monitoring({
      project: 'pulse-publisher',
      credentials: {},
      mock: true,
    });

    // Set options on exchanges
    exchanges.configure({
      validator:              validate,
      credentials:            cfg.pulse
    });
  });

  // Test that we can connect to AMQP server
  test("connect", function() {
    return exchanges.connect().then(function(publisher) {
      assert(publisher instanceof subject.Publisher,
             "Should get an instance of exchanges.Publisher");
    });
  });

  // Test that we can publish messages
  test("publish message", function() {
    return exchanges.connect().then(function(publisher) {
      return publisher.testExchange({someString: "My message"}, {
        testId:           "myid",
        taskRoutingKey:   "some.string.with.dots",
        state:            undefined // Optional
      });
    });
  });


  /*
  // Test that we can publish messages fast
  test("publish message 400", function() {
    var promises = [];
    return exchanges.connect().then(function(publisher) {
      for (var i = 0; i < 400; i++) {
        promises.push(new Promise(function(accept) {
          setTimeout(accept, Math.floor(i / 4));
        }).then(function() {
          return publisher.testExchange({someString: "My message" + i}, {
            testId:           "myid",
            taskRoutingKey:   "some.string.with.dots",
            state:            undefined // Optional
          }).then(function() {
            publisher.testExchange({someString: "My message" + i}, {
              testId:           "myid",
              taskRoutingKey:   "some.string.with.dots",
              state:            undefined // Optional
            });
          });
        }));
      }
      return Promise.all(promises);
    });
  }); return; // */

  /*
  // Test that we can publish messages fast
  test("publish message 400", function() {
    var promises = [];
    return exchanges.connect().then(function(publisher) {
      for (var i = 0; i < 400; i++) {
        promises.push(publisher.testExchange({someString: "My message" + i}, {
          testId:           "myid",
          taskRoutingKey:   "some.string.with.dots",
          state:            undefined // Optional
        }));
      }
      return Promise.all(promises);
    });
  }); return; // */

  /*
  // Test that we can publish messages fast (for TCP Nagle disable test)
  test("publish message", function() {
    return exchanges.connect().then(function(publisher) {
      var i = 0;
      var loop = function() {
        i += 1;
        if (i > 400) {
          return;
        }
        console.log(i);
        return publisher.testExchange({someString: "My message"}, {
          testId:           "myid",
          taskRoutingKey:   "some.string.with.dots",
          state:            undefined // Optional
        }).then(function() {
          return loop();
        });
      };
      return loop();
    });
  }); return; // */

  // Test that we can publish messages
  test("publish message w. number in routing key", function() {
    return exchanges.connect().then(function(publisher) {
      return publisher.testExchange({someString: "My message"}, {
        testId:           "myid",
        taskRoutingKey:   "some.string.with.dots",
        state:            undefined, // Optional
        index:            15
      });
    });
  });

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

  // Test publication fails on size violation
  test("publish error w. size violation", function() {
    return exchanges.connect().then(function(publisher) {
      return publisher.testExchange({
        someString:   "My message",
      }, {
        testId:           "myid-this-is-more-tahn-22-chars-long",
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

  // Test publication fails on multiple words
  test("publish error w. multiple words", function() {
    return exchanges.connect().then(function(publisher) {
      return publisher.testExchange({
        someString:   "My message",
      }, {
        testId:           "not.single.word",
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

  // Test that we can publish messages and get them again
  test("publish message (and receive)", function() {
    var conn,
        channel,
        queue = 'queue/' + cfg.pulse.username + '/test/' + slugid.v4();
    var messages = [];
    return amqplib.connect(connectionString).then(function(conn_) {
      conn = conn_;
      return conn.createConfirmChannel();
    }).then(function(channel_) {
      channel = channel_;
      return channel.assertQueue(queue, {
        exclusive:  true,
        durable:    false,
        autoDelete: true,
      });
    }).then(function() {
      var testExchange = 'exchange/' + cfg.pulse.username +
                         '/test-exchange';
      return channel.bindQueue(queue, testExchange, 'myid.#');
    }).then(function() {
      return channel.consume(queue, function(msg) {
        msg.content = JSON.parse(msg.content.toString());
        //console.log(JSON.stringify(msg, null, 2));
        messages.push(msg);
      });
    }).then(function() {
      return exchanges.connect().then(function(publisher) {
        return publisher.testExchange({someString: "My message"}, {
          testId:           "myid",
          taskRoutingKey:   "some.string.with.dots",
          state:            undefined // Optional
        });
      });
    }).then(function() {
      return new Promise(function(accept) {setTimeout(accept, 400);});
    }).then(function() {
      // Others could be publishing to this exchange, so we check msgs > 0
      assert(messages.length > 0, "Didn't get exactly any messages");
    });
  });

  // Test that we can publish messages and get them again
  test("publish message (and receive by CC)", function() {
    var conn,
        channel,
        queue = 'queue/' + cfg.pulse.username + '/test/' + slugid.v4();
    var messages = [];
    return amqplib.connect(connectionString).then(function(conn_) {
      conn = conn_;
      return conn.createConfirmChannel();
    }).then(function(channel_) {
      channel = channel_;
      return channel.assertQueue(queue, {
        exclusive:  true,
        durable:    false,
        autoDelete: true,
      });
    }).then(function() {
      var testExchange = 'exchange/' + cfg.pulse.username +
                         '/test-exchange';
      return Promise.all([
        channel.bindQueue(queue, testExchange, 'something.cced')
      ]);
    }).then(function() {
      return channel.consume(queue, function(msg) {
        msg.content = JSON.parse(msg.content.toString());
        //console.log(JSON.stringify(msg, null, 2));
        messages.push(msg);
      });
    }).then(function() {
      return exchanges.connect().then(function(publisher) {
        return publisher.testExchange({someString: "My message"}, {
          testId:           "myid",
          taskRoutingKey:   "some.string.with.dots",
          state:            undefined // Optional
        });
      });
    }).then(function() {
      return new Promise(function(accept) {setTimeout(accept, 300);});
    }).then(function() {
      assert(messages.length === 1, "Didn't get exactly one message");
    });
  });

  // Test that we record statistics
  test("publish message (record statistics)", function() {
    assert(_.keys(monitor.counts).length === 0, "We shouldn't have any points");
    return exchanges.connect({
      monitor,
    }).then(function(publisher) {
      return publisher.testExchange({someString: "My message"}, {
        testId:           "myid",
        taskRoutingKey:   "some.string.with.dots",
        state:            undefined // Optional
      });
    }).then(function() {
      assert(_.keys(monitor.counts).length === 1, "We should have one point");
    });
  });
});
