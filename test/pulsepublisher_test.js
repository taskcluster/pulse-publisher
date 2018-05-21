const assert     = require('assert');
const subject    = require('../');
const config     = require('typed-env-config');
const monitoring = require('taskcluster-lib-monitor');
const validator  = require('taskcluster-lib-validate');
const path       = require('path');
const fs         = require('fs');
const debug      = require('debug')('test');
const slugid     = require('slugid');
const amqplib    = require('amqplib');
const _          = require('lodash');
const libUrls    = require('taskcluster-lib-urls');

suite('Publish to Pulse', function() {
  // Load necessary configuration
  var cfg = config({});

  if (!cfg.pulse.password) {
    console.log('Skipping tests due to missing cfg.pulse');
    this.pending = true;
  }

  // ConnectionString for use with amqplib only
  var connectionString = [
    'amqps://',         // Ensure that we're using SSL
    cfg.pulse.username,
    ':',
    cfg.pulse.password,
    '@',
    cfg.pulse.hostname,
    ':',
    5671,                // Port for SSL
    '/',
    encodeURIComponent(cfg.pulse.vhost),
  ].join('');

  var monitor = null;
  var exchanges = null;
  var publisher = null;
  setup(async function() {
    exchanges = new subject({
      name:               'test',
      version:            'v1',
      title:              'Title for my Events',
      description:        'Test exchanges used for testing things only',
    });
    // Check that we can declare an exchange
    exchanges.declare({
      exchange:           'test-exchange',
      name:               'testExchange',
      title:              'Test Exchange',
      description:        'Place we post message for **testing**.',
      routingKey: [
        {
          name:           'testId',
          summary:        'Identifier that we use for testing',
          multipleWords:  false,
          required:       true,
          maxSize:        22,
        }, {
          name:           'taskRoutingKey',
          summary:        'Test specific routing-key: `test.key`',
          multipleWords:  true,
          required:       true,
          maxSize:        128,
        }, {
          name:           'state',
          summary:        'State of something',
          multipleWords:  false,
          required:       false,
          maxSize:        16,
        }, {
          name:           'index',
          summary:        'index of something',
          multipleWords:  false,
          required:       false,
          maxSize:        3,
        }, {
          name:           'myConstant',
          summary:        'Some constant to test',
          constant:       '-constant-',
        },
      ],
      schema:             'exchange-test-schema.yml',
      messageBuilder:     function(msg) { return msg; },
      routingKeyBuilder:  function(msg, rk) { return rk; },
      CCBuilder:          function(msg, rk, cc = []) {return cc;},
    });

    var validate = await validator({
      rootUrl: libUrls.testRootUrl(),
      serviceName: 'test',
      folder:  path.join(__dirname, 'schemas'),
    });

    monitor = await monitoring({
      projectName: 'pulse-publisher',
      credentials: {},
      mock: true,
    });

    // Set options on exchanges
    exchanges.configure({validator: validate});

    publisher = await exchanges.connect({
      rootUrl: libUrls.testRootUrl(),
      credentials: cfg.pulse,
    });
  });

  teardown(function() {
    publisher.close();
  });

  // Test that we can connect to AMQP server
  test('connect', function() {
    assert(publisher instanceof subject.Publisher,
      'Should get an instance of exchanges.Publisher');
  });

  // Test that we can publish messages
  test('publish message', function() {
    return publisher.testExchange({someString: 'My message'}, {
      testId:           'myid',
      taskRoutingKey:   'some.string.with.dots',
      state:            undefined, // Optional
    });
  });

  // Test that we can publish messages
  test('publish message w. number in routing key', function() {
    return publisher.testExchange({someString: 'My message'}, {
      testId:           'myid',
      taskRoutingKey:   'some.string.with.dots',
      state:            undefined, // Optional
      index:            15,
    });
  });

  // Test publication fails on schema violation
  test('publish error w. schema violation', function() {
    try {
      return publisher.testExchange({
        someString:   'My message',
        volation:   true,
      }, {
        testId:           'myid',
        taskRoutingKey:   'some.string.with.dots',
        state:            undefined, // Optional
      });
    } catch (err) {
      // Expected an error
      debug('Got expected Error: %s, %j', err, err);
      return;
    }
    assert(false, 'Expected an error');
  });

  // Test publication fails on required key missing
  test('publish error w. required key missing', function() {
    try {
      return publisher.testExchange({
        someString:   'My message',
      }, {
        taskRoutingKey:   'some.string.with.dots',
        state:            'here',
      });
    } catch (err) {
      // Expected an error
      debug('Got expected Error: %s, %j', err, err);
      return;
    }
    assert(false, 'Expected an error');
  });

  // Test publication fails on size violation
  test('publish error w. size violation', function() {
    try {
      return publisher.testExchange({
        someString:   'My message',
      }, {
        testId:           'myid-this-is-more-tahn-22-chars-long',
        taskRoutingKey:   'some.string.with.dots',
        state:            undefined, // Optional
      });
    } catch (err) {
      // Expected an error
      debug('Got expected Error: %s, %j', err, err);
      return;
    }
    assert(false, 'Expected an error');
  });

  // Test publication fails on multiple words
  test('publish error w. multiple words', function() {
    try {
      return publisher.testExchange({
        someString:   'My message',
      }, {
        testId:           'not.single.word',
        taskRoutingKey:   'some.string.with.dots',
        state:            undefined, // Optional
      });
    } catch (err) {
      // Expected an error
      debug('Got expected Error: %s, %j', err, err);
      return;
    }
    assert(false, 'Expected an error');
  });

  // Test that we can publish messages and get them again
  test('publish message (and receive)', function() {
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
                         '/v1/test-exchange';
      return channel.bindQueue(queue, testExchange, 'myid.#');
    }).then(function() {
      return channel.consume(queue, function(msg) {
        msg.content = JSON.parse(msg.content.toString());
        messages.push(msg);
      });
    }).then(function() {
      return publisher.testExchange({someString: 'My message'}, {
        testId:           'myid',
        taskRoutingKey:   'some.string.with.dots',
        state:            undefined, // Optional
      });
    }).then(function() {
      return new Promise(function(accept) {setTimeout(accept, 400);});
    }).then(function() {
      // Others could be publishing to this exchange, so we check msgs > 0
      assert(messages.length > 0, 'Didn\'t get any messages');
    }).finally(function() {
      if (conn) {
        return conn.close();
      }
    });
  });

  // Test that we can publish messages and get them again
  test('publish message (and receive by CC)', function() {
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
                         '/v1/test-exchange';
      return Promise.all([
        channel.bindQueue(queue, testExchange, 'something.cced'),
      ]);
    }).then(function() {
      return channel.consume(queue, function(msg) {
        msg.content = JSON.parse(msg.content.toString());
        //console.log(JSON.stringify(msg, null, 2));
        messages.push(msg);
      });
    }).then(function() {
      return publisher.testExchange({someString: 'My message'}, {
        testId:           'myid',
        taskRoutingKey:   'some.string.with.dots',
        state:            undefined, // Optional
      }, ['something.cced']);
    }).then(function() {
      return new Promise(function(accept) {setTimeout(accept, 300);});
    }).then(function() {
      assert(messages.length === 1, 'Didn\'t get exactly one message');
    }).finally(function() {
      return conn.close();
    });
  });

  // Test that we can publish messages with large CC
  test('publish message (huge CC)', function() {
    return publisher.testExchange({someString: 'My message'}, {
      testId:           'myid',
      taskRoutingKey:   'some.string.with.dots',
      state:            undefined, // Optional
    }, [
      /* eslint-disable max-len */
      'route.index.gecko.v2.mozilla-aurora.revision.4b053b4106a9b99268312c5fcf8ac1048cc80430.firefox-l10n.linux64-opt.son',
      'route.index.gecko.v2.mozilla-aurora.pushdate.2017.01.23.20170123201356.firefox-l10n.linux64-opt.son',
      'route.index.gecko.v2.mozilla-aurora.latest.firefox-l10n.linux64-opt.son',
      'route.index.gecko.v2.mozilla-aurora.revision.4b053b4106a9b99268312c5fcf8ac1048cc80430.firefox-l10n.linux64-opt.sq',
      'route.index.gecko.v2.mozilla-aurora.pushdate.2017.01.23.20170123201356.firefox-l10n.linux64-opt.sq',
      'route.index.gecko.v2.mozilla-aurora.latest.firefox-l10n.linux64-opt.sq',
      'route.index.gecko.v2.mozilla-aurora.revision.4b053b4106a9b99268312c5fcf8ac1048cc80430.firefox-l10n.linux64-opt.sr',
      'route.index.gecko.v2.mozilla-aurora.pushdate.2017.01.23.20170123201356.firefox-l10n.linux64-opt.sr',
      'route.index.gecko.v2.mozilla-aurora.latest.firefox-l10n.linux64-opt.sr',
      'route.index.gecko.v2.mozilla-aurora.revision.4b053b4106a9b99268312c5fcf8ac1048cc80430.firefox-l10n.linux64-opt.sv-SE',
      'route.index.gecko.v2.mozilla-aurora.pushdate.2017.01.23.20170123201356.firefox-l10n.linux64-opt.sv-SE',
      'route.index.gecko.v2.mozilla-aurora.latest.firefox-l10n.linux64-opt.sv-SE',
      'route.index.gecko.v2.mozilla-aurora.revision.4b053b4106a9b99268312c5fcf8ac1048cc80430.firefox-l10n.linux64-opt.ta',
      /* eslint-enable max-len */
    ]);
  });

  // Test that we record statistics
  test('publish message (record statistics)', function() {
    var _publisher;
    assert(_.keys(monitor.counts).length === 0, 'We shouldn\'t have any points');
    return exchanges.connect({
      rootUrl: libUrls.testRootUrl(),
      monitor,
      credentials: cfg.pulse,
    }).then(function(publisher) {
      _publisher = publisher;
      return publisher.testExchange({someString: 'My message'}, {
        testId:           'myid',
        taskRoutingKey:   'some.string.with.dots',
        state:            undefined, // Optional
      });
    }).then(function() {
      assert(_.keys(monitor.counts).length === 1, 'We should have one point');
      return _publisher.close();
    });
  });

  // skipped because tc-pulse isn't running
  // https://bugzilla.mozilla.org/show_bug.cgi?id=1436456
  suite.skip('with taskcluster credentials', function() {
    if (!cfg.taskcluster) {
      console.log('Skipping tests due to missing cfg.taskcluster');
      this.pending = true;
    }

    test('publish message (and receive)', async function() {
      var queue = 'queue/' + cfg.pulse.username + '/test/' + slugid.v4(),
        namespace = 'taskcluster-tests-' + slugid.nice();
      var messages = [];

      let publisher = await exchanges.connect({
        credentials: cfg.taskcluster,
        namespace,
        expires: '1 hour',
      });

      let conn = await amqplib.connect(connectionString);
      let channel = await conn.createConfirmChannel();
      await channel.assertQueue(queue, {
        exclusive:  true,
        durable:    false,
        autoDelete: true,
      });
      var testExchange = 'exchange/' + namespace + '/v1/test-exchange';
      await channel.bindQueue(queue, testExchange, 'myid.#');
      await channel.consume(queue, function(msg) {
        msg.content = JSON.parse(msg.content.toString());
        //console.log(JSON.stringify(msg, null, 2));
        messages.push(msg);
      });

      await publisher.testExchange({someString: 'My message'}, {
        testId:           'myid',
        taskRoutingKey:   'some.string.with.dots',
        state:            undefined, // Optional
      });
      await new Promise(function(accept) {setTimeout(accept, 400);});
      assert(messages.length == 1, 'Didn\'t get exactly one message');
      await publisher.close();
    });
  });
});
