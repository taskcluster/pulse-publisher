suite('Exchanges (Publish on Pulse w. schemaPrefix)', function() {
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

  // Load necessary configuration
  var cfg = config({});

  if (!cfg.pulse.password) {
    console.log('Skipping \'pulse publisher\', missing config file: user-config.yml');
    this.pending = true;
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
    5671,                // Port for SSL
  ].join('');

  var monitor = null;
  var exchanges = null;
  var publisher = null;
  setup(async function() {
    exchanges = new subject({
      title:              'Title for my Events',
      description:        'Test exchanges used for testing things only',
      schemaPrefix:       'http://localhost:1203/schemas/pulse-publisher-tests/',
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
      schema:             'exchange-test-schema.json#',
      messageBuilder:     function(msg) { return msg; },
      routingKeyBuilder:  function(msg, rk) { return rk; },
      CCBuilder:          function() {return ['something.cced'];},
    });

    var validate = await validator({
      rootUrl:     'http://localhost:1203/',
      serviceName: 'pulse-publisher-tests',
      folder:      path.join(__dirname, 'schemas'),
    });

    monitor = await monitoring({
      projectName: 'pulse-publisher',
      credentials: {},
      mock: true,
    });

    // Set options on exchanges
    exchanges.configure({
      validator:              validate,
      credentials:            cfg.pulse,
    });

    publisher = await exchanges.connect();
  });

  teardown(function() {
    return publisher.close();
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
});
