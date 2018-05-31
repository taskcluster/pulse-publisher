suite('Exchanges', function() {
  var assert  = require('assert');
  var subject = require('../');
  var config  = require('typed-env-config');
  var aws     = require('aws-sdk');

  var cfg = config({});

  if (!cfg.aws.secretAccessKey || !cfg.testBucket) {
    console.log('Skipping \'publish\', missing S3 configs in: user-config.yml');
    this.pending = true;
  }

  test('publish', function() {
    // Create an exchanges
    var exchanges = new subject({
      serviceName:        'test',
      projectName:        'taskcluster-test',
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
        },
      ],
      schema: 'exchanges-test.yml',
      messageBuilder:     function(test) { return test; },
      routingKeyBuilder:  function(test, state) {
        return {
          testId:           test.id,
          taskRoutingKey:   test.key,
          state:            state,
        };
      },
      CCBuilder:          function() { return []; },
    });

    // Publish
    return exchanges.publish({
      rootUrl: 'https://test-bucket-for-any-garbage',
      aws:                  cfg.aws,
    }).then(function() {
      // Get the file... we don't bother checking the contents this is good
      // enough
      var s3 = new aws.S3(cfg.aws);
      return s3.getObject({
        Bucket:     'test-bucket-for-any-garbage',
        Key:        'references/test/v1/exchanges.json',
      }).promise();
    }).then(function(res) {
      var reference = JSON.parse(res.Body);
      assert(reference.entries, 'Missing entries');
      assert(reference.entries.length > 0, 'Has no entries');
      assert(reference.title, 'Missing title');
    });
  });
});
