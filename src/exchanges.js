var url           = require('url');
var assert        = require('assert');
var debug         = require('debug')('base:exchanges');
var _             = require('lodash');
var Promise       = require('promise');
var path          = require('path');
var fs            = require('fs');
var Ajv           = require('ajv');
var aws           = require('aws-sdk');
var amqplib       = require('amqplib');
var events        = require('events');
var util          = require('util');
var common        = require('./common');
var taskcluster   = require('taskcluster-client');
var libUrls       = require('taskcluster-lib-urls');

// wait 30 seconds before closing a channel, to allow pending operations to flush
var CLOSE_DELAY = 30 * 1000;

// unconditionally reconnect to pulse on this interval; this ensures the
// reconnecting logic gets exercised
var RECONNECT_INTERVAL = '6 hours';

/** Class for publishing to a set of declared exchanges */
var Publisher = function(entries, exchangePrefix, connectionFunc, options) {
  events.EventEmitter.call(this);
  assert(options.validator, 'options.validator must be provided');
  this._conn = null;
  this.__reconnectTimer = null;
  this._connectionFunc = connectionFunc;
  this._channel = null;
  this._connecting = null;
  this._entries = entries;
  this._exchangePrefix = exchangePrefix;
  this._options = options;
  this._errCount = 0;
  this._lastErr = Date.now();
  this._lastTime = 0;
  this._sleeping = null;
  this._sleepingTimeout = null;
  if (options.drain || options.component) {
    console.log('taskcluster-lib-stats is now deprecated!\n' +
                'Use the `monitor` option rather than `drain`.\n' +
                '`monitor` should be an instance of taskcluster-lib-monitor.\n' +
                '`component` is no longer needed. Prefix your `monitor` before use.');
  }

  var monitor = null;
  if (options.monitor) {
    monitor = options.monitor;
  }

  entries.forEach((entry) => {
    this[entry.name] = (...args) => {
      // Construct message and routing key from arguments
      var message = entry.messageBuilder.apply(undefined, args);
      common.validateMessage(this._options.rootUrl, this._options.serviceName, this._options.version,
        options.validator, entry, message);

      var routingKey = common.routingKeyToString(entry, entry.routingKeyBuilder.apply(undefined, args));

      var CCs = entry.CCBuilder.apply(undefined, args);
      assert(CCs instanceof Array, 'CCBuilder must return an array');

      // Serialize message to buffer
      var payload = new Buffer(JSON.stringify(message), 'utf8');

      // Find exchange name
      var exchange = exchangePrefix + entry.exchange;

      // Log that we're publishing a message
      debug('Publishing message on exchange: %s', exchange);

      // Return promise
      return this._connect().then(channel => {
        return new Promise((accept, reject) => {
          // Start timer
          var start = null;
          if (monitor) {
            start = process.hrtime();
          }

          // Set a timeout
          let done = false;
          this._sleep12Seconds().then(() => {
            if (!done) {
              let err = new Error('publish message timed out after 12s');
              this._handleError(err);
              reject(err);
            }
          });

          // Publish message
          channel.publish(exchange, routingKey, payload, {
            persistent:         true,
            contentType:        'application/json',
            contentEncoding:    'utf-8',
            CC:                 CCs,
          }, (err, val) => {
            // NOTE: many channel errors will not invoke this callback at all,
            // hence the 12-second timeout
            done = true;
            if (monitor) {
              var d = process.hrtime(start);
              monitor.measure(exchange, d[0] * 1000 + d[1] / 1000000);
              monitor.count(exchange);
            }

            // Handle errors
            if (err) {
              err.methodName = entry.name;
              err.exchange = exchange;
              err.routingKey = routingKey;
              err.payload = payload;
              err.ccRoutingKeys = CCs;
              debug('Failed to publish message: %j and routingKey: %s, ' +
              'with error: %s, %j', message, routingKey, err, err);
              if (monitor) {
                monitor.reportError(err);
              }
              return reject(err);
            }
            accept(val);
          });
        });
      });
    };
  });
};

// Inherit from events.EventEmitter
util.inherits(Publisher, events.EventEmitter);

// Hack to get promises that resolve after 12s without creating a setTimeout
// for each, instead we create a new promise every 2s and reuse that.
Publisher.prototype._sleep12Seconds = function() {
  let time = Date.now();
  if (time - this._lastTime > 2000) {
    this._sleeping = new Promise(accept => {
      this._sleepingTimeout = setTimeout(accept, 12 * 1000);
    });
  }
  return this._sleeping;
};

Publisher.prototype._handleError = function(err) {
  // Reset error count, if last error is more than 15 minutes ago
  if (Date.now() - this._lastErr > 15 * 60 * 1000) {
    this._lastErr = Date.now();
    this._errCount = 0;
  }
  this._lastErr = Date.now();
  // emit error and abort, if we've retried more than 5 times
  if (this._errCount++ > 5) {
    this.emit('error', err);
    return;
  }

  // report warning
  if (this._options.monitor) {
    this._options.monitor.reportError(err, 'warning');
  }

  // Close existing connection
  if (this._conn) {
    this._conn.___closing = true;
    this._conn.close();
  }

  // Reconnect
  this._connecting = null;
  return this._connect();
};

Publisher.prototype._connect = async function() {
  if (this._connecting) {
    return this._connecting;
  }
  return this._connecting = (async () => {
    let {connectionString, reconnectAt} = await this._connectionFunc();

    // Create connection
    let retry = 0;
    while (true) {
      // Try to connect a few times, as DNS randomization is used to ensure we try
      // different nodes
      try {
        this._conn = await amqplib.connect(connectionString, {
          // Disable TCP Nagle, test don't show any difference in performance, but
          // it probably can't hurt to disable Nagle, this is a low bandwidth
          // application, so it makes a lot of sense to disable Nagle.
          noDelay: true,
          timeout: 30 * 1000,
        });
      } catch (err) {
        if (retry++ < 12) {
          continue; // try again
        }
        throw err;
      }
      break;
    }

    // Create confirm publish channel
    this._channel = await this._conn.createConfirmChannel();

    // Create exchanges as declared
    await Promise.all(this._entries.map(entry => {
      var name = this._exchangePrefix + entry.exchange;
      return this._channel.assertExchange(name, 'topic', {
        durable:      this._options.durableExchanges,
        internal:     false,
        autoDelete:   false,
      });
    }));

    this._channel.on('error', (err) => {
      debug('Channel error in Publisher: ', err.stack);
      this._handleError(err);
    });
    this._conn.on('error', (err) => {
      debug('Connection error in Publisher: ', err.stack);
      this._handleError(err);
    });
    // Handle graceful server initiated shutdown as an error
    let conn = this._conn;
    conn.___closing = false;
    this._channel.on('close', () => {
      if (conn.___closing) {
        return;
      }
      debug('Channel closed unexpectedly');
      this._handleError(new Error('channel closed unexpectedly'));
    });
    this._conn.on('close', () => {
      if (conn.___closing) {
        return;
      }
      debug('Connection closed unexpectedly');
      this._handleError(new Error('connection closed unexpectedly'));
    });

    // set up to reconnect soon..
    debug('Will reconnect at ' + reconnectAt.toJSON());
    let reconnectDelay = Math.max(0, reconnectAt - new Date());
    if (this.__reconnectTimer) {
      clearTimeout(this.__reconnectTimer);
    }
    this.__reconnectTimer = setTimeout(() => this._reconnect(), reconnectDelay);

    return this._channel;
  })().catch(err => {
    // Try again, if limit isn't hit
    return this._handleError(err);
  });
};

Publisher.prototype._reconnect = async function() {
  debug('reconnecting to Pulse');
  this._connecting = null;

  // close old connection after a delay, to allow any pending operations to
  // complete
  if (this._conn) {
    let oldConn = this._conn;
    this._conn = null;

    setTimeout(() => {
      oldConn.___closing = true;
      oldConn.close();
    }, CLOSE_DELAY);
  }

  // start connecting; errors here will be handled via _handleError, so
  // the resulting Promise can be ignored
  this._connect();
};

/** Close the connection */
Publisher.prototype.close = async function() {
  if (this._connecting) {
    await this._connecting;
  }
  if (this.__reconnectTimer) {
    clearTimeout(this.__reconnectTimer);
    this.__reconnectTimer = null;
  }
  if (this._sleepingTimeout) {
    clearTimeout(this._sleepingTimeout);
    this._sleepingTimeout = null;
  }
  if (this._conn) {
    this._connecting = null;
    this._conn.___closing = true;
    return this._conn.close();
  }
};

/** Create a collection of exchange declarations
 *
 * options:
 * {
 *   serviceName: 'foo',
 *   version: 'v1',
 *   title: "Title of documentation page",
 *   description: "Description in markdown",
 *   durableExchanges: true || false // If exchanges are durable (default true)
 * }
 */
var Exchanges = function(options) {
  this._entries = [];
  this._options = {
    durableExchanges:     true,
  };
  assert(options.serviceName, 'serviceName must be provided');
  assert(options.projectName, 'projectName must be provided');
  assert(options.version,     'version must be provided');
  assert(options.title,       'title must be provided');
  assert(options.description, 'description must be provided');
  assert(!options.exchangePrefix, 'exchangePrefix is not allowed');
  assert(!options.schemaPrefix, 'schemaPrefix is not allowed');
  this.configure(options);
};

/** Declare an new exchange
 *
 * options:
 * {
 *   exchange:     'exchange-name',      // exchange identifier on AMQP
 *   name:         "nameForClients",     // name usable in client APIs
 *   title:        "Exchange title",
 *   description:  "Exchange description in markdown",
 *   routingKey: [ // Description of words, that make up the routing key
 *     {
 *       name:           'name_of_key',  // name of key for client APIs
 *       summary:        "Details in **markdown**",  // For documentation
 *       multipleWords:  true || false,  // true, if entry can contain dot
 *       required:       true || false,  // true, if a value is required
 *       constant:      'constant',      // Constant value, always this value
 *       maxSize:        22,             // Maximum size of word
 *     },
 *     // More entries...
 *   ],
 *   schema:       'foo-message.yml'     // Message schema (a file in schemas/v1)
 *   messageBuilder: function() {...}    // Return message from arguments given
 *   routingKeyBuilder: function() {...} // Return routing key from arguments
 *   CCBuilder: function() {...}         // Return list of CC'ed routing keys
 * }
 *
 * Remark, it is only possible to have one routing key entry that has the
 * multipleWords entry set to true. This restriction is necessary to facilitate
 * automatic parsing of the routing key.
 *
 * When a publisher is constructor with `connect` the `name` from options will
 * be the identifier for the method used to publish messages. The arguments
 * passed to this method will be passed to both `messageBuilder` and
 * `routingKeyBuilder`.
 *
 * Note, `routingKeyBuilder` may return either a string, or an object mapping
 * from name of routingKey entries to string values. If returning an object
 * then `maxSize` will be checked for all entries, as will `required`, and if
 * `required` is `false` the entry will default to `_` if no value is provided.
 * (It's not recommended to return a string).
 */
Exchanges.prototype.declare = function(options) {
  assert(options, 'options must be given to declare');

  // Check that we have properties that must be strings
  [
    'exchange', 'name', 'title', 'description', 'schema',
  ].forEach(function(key) {
    assert(typeof options[key] === 'string', 'Option: \'' + key + '\' must be ' +
           'a string');
  });

  assert(!options.schema.startsWith('http'), 'schema must be a bare filename in schemas/v1');
  options.schema = `${this._options.version}/${options.schema.replace(/ya?ml$/, 'json#')}`;

  // Validate routingKey declaration
  assert(options.routingKey instanceof Array,
    'routingKey must be an array');

  var keyNames = [];
  var sizeLeft = 255;
  var firstMultiWordKey = null;
  options.routingKey.forEach(function(key) {
    // Check that the key name is unique
    assert(keyNames.indexOf(key.name) === -1, 'Can\'t have two routing key ' +
           'entries named: \'' + key.name + '\'');
    keyNames.push(key.name);
    // Check that we have a summary
    assert(typeof key.summary === 'string', 'summary of routingKey entry ' +
           'must be provided.');

    // Ensure that have a boolean value for simplicity
    key.multipleWords = key.multipleWords ? true : false;
    key.required      = key.required ? true : false;

    // Check that we only have one multipleWords key in the routing key. If we
    // have more than one then we can't really parse the routing key
    // automatically. And technically, there is probably little need for two
    // multiple word routing key entries.
    // Note: if the need arises we should probably consider CC'ing multiple
    // routing keys, or something like that. At least that is a possible cleaner
    // design solution.
    if (key.multipleWords) {
      assert(firstMultiWordKey === null,
        'Can\'t have two multipleWord entries in a routing key, ' +
             'here we have both \'' + firstMultiWordKey + '\' and ' +
             '\'' + key.name + '\'');
      firstMultiWordKey = key.name;
    }

    if (key.constant) {
      // Check that any constant is indeed a string
      assert(typeof key.constant === 'string',
        'constant must be a string, if provided');

      // Set maxSize
      if (!key.maxSize) {
        key.maxSize = key.constant.length;
      }
    }

    // Check that we have a maxSize
    assert(typeof key.maxSize == 'number' && key.maxSize > 0,
      'routingKey declaration ' + key.name + ' must have maxSize > 0');

    // Check size left in routingKey space
    if (sizeLeft != 255) {
      sizeLeft -= 1; // Remove on for the joining dot
    }
    sizeLeft -= key.maxSize;
    assert(sizeLeft >= 0, 'Combined routingKey cannot be larger than 255 ' +
           'including joining dots');
  });

  // Validate messageBuilder
  assert(options.messageBuilder instanceof Function,
    'messageBuilder must be a Function');

  // Validate routingKeyBuilder
  assert(options.routingKeyBuilder instanceof Function,
    'routingKeyBuilder must be a function');

  // Validate CCBuilder
  assert(options.CCBuilder instanceof Function,
    'CCBuilder must be a function');

  // Check that `exchange` and `name` are unique
  this._entries.forEach(function(entry) {
    assert(entry.exchange !== options.exchange,
      'Cannot have two declarations with exchange: \'' +
           entry.exchange + '\'');
    assert(entry.name !== options.name,
      'Cannot have two declarations with name: \'' + entry.name + '\'');
  });

  // Add options to set of options
  this._entries.push(options);
};

/** Configure the events declaration */
Exchanges.prototype.configure = function(options) {
  this._options = _.defaults({}, options, this._options);
};

/**
 * Connect by AMQP and create a publisher
 *
 * Options:
 * {
 *   rootUrl:                    // Taskcluster rootUrl for this service
 *   credentials: .. (see below)
 *   namespace: '...',           // pulse namespace (usually taskcluster-foo)
 *   expires: '1 year',           // time after which the namespace expires
 *   contact: 'foo@bar',         // contact email for the pulse namespace
 *   validator:                  // Instance of base.validator
 *   monitor:           await require('taskcluster-lib-monitor')({...}),
 * }
 *
 * Given a set of permanent Pulse credentials, pass credentials:
 * {
 *   username:        '...',   // Pulse username
 *   password:        '...',   // Pulse password
 *   hostname:        '...'    // Hostname, defaults to pulse.mozilla.org
 *   vhost:           '/',     // Vhost
 * },
 * In this case, the namespace will default to the username.
 *
 * To use Taskcluster-Pulse, pass credentials:
 * {
 *   clientId: '...', // client with scope `pulse:claim-namespace:<namespace>`
 *   accessToken: '...',
 *   certificate: '...', // if using temporary credentials
 * }
 *
 * For a fake publisher, pass credentials: {fake: true}.
 *
 * This method will connect to AMQP server and return a instance of Publisher.
 * The publisher will have a method for each declared exchange, the method
 * will carry the `name` given when the exchange was declared.
 *
 * In case of connection or internal errors the publisher will emit the `error`
 * event and all further attempts to use it will fail. In the future we may
 * implement a form of reconnection, but for now, just leave the `error` events
 * unhandled and let the process restart on its own.
 *
 * If credentials are {fake: true}, then no pulse connections will be made. Instead,
 * the publisher object will emit a 'fakePublish' event with {exchange, routingKey,
 * payload, CCs}.
 *
 * Return a promise for an instance of `Publisher`.
 */
Exchanges.prototype.connect = async function(options) {
  options = _.defaults({}, options || {}, this._options, {
    credentials:        {},
  });

  assert(options.rootUrl, 'A rootUrl function must be provided.');
  assert(options.validator, 'A validator must be provided.');
  assert(options.credentials, 'Some kind of credentials are required.');
  let credentials = options.credentials;
  if (!credentials.fake) {
    assert(options.namespace, 'Must provide a namespace.');
    assert(credentials.username, 'Must provide a username.');
    assert(credentials.password, 'Must provide a password.');
    assert(credentials.hostname, 'Must provide a hostname.');
    assert(credentials.vhost, 'Must provide a vhost.');
  }

  // Find exchange prefix
  const namespace = options.namespace || 'taskcluster-fake';
  if (process.env.NODE_ENV === 'production') {
    assert(namespace === options.projectName, 'in production, the namespace must match the projectName');
  }
  var exchangePrefix = [
    'exchange',
    namespace,
    options.version,
    '',
  ].join('/');

  // Clone entries for consistency
  var entries = _.cloneDeep(this._entries);

  // make a function to get a connectionString, based on options.
  let connectionFunc;
  if (credentials.username &&
      credentials.password) {
    let connectionString = [
      'amqps://',         // Ensure that we're using SSL
      credentials.username,
      ':',
      credentials.password,
      '@',
      credentials.hostname,
      ':',
      5671,                // Port for SSL
      '/',
      encodeURIComponent(credentials.vhost),
    ].join('');
    connectionFunc = async () => ({
      connectionString,
      reconnectAt: taskcluster.fromNow(RECONNECT_INTERVAL),
    });
  } else if (credentials.clientId && credentials.accessToken) {
    assert(options.namespace, 'Must specify a namespace');
    assert(options.expires, 'Must specify a namespace expiration');

    let tcPulse = new taskcluster.Pulse({credentials});
    connectionFunc = async () => {
      let claim = await tcPulse.claimNamespace(options.namespace, {
        expires: taskcluster.fromNow(options.expires),
        contact: options.contact,
      });
      return {
        connectionString: claim.connectionString,
        reconnectAt: new Date(claim.reclaimAt),
      };
    };
  } else if (credentials.fake) {
    // only load fake on demand
    var FakePublisher = require('./fake');
    return new FakePublisher(entries, exchangePrefix, options);
  } else {
    throw new Error('invalid credentials');
  }

  // return publisher
  let publisher = new Publisher(entries, exchangePrefix, connectionFunc, options);
  await publisher._connect();
  return publisher;
};

/**
 * Return reference as JSON for the declared exchanges.  This is based only on the declared
 * exchange information.
 */
Exchanges.prototype.reference = function() {
  // Build exchange prefix based on projectName
  var exchangePrefix = [
    'exchange',
    this._options.projectName,
    this._options.version,
    '',
  ].join('/');

  // Check title and description
  assert(this._options.title,       'title must be provided');
  assert(this._options.description, 'description must be provided');

  // Create reference
  var reference = {
    version:            0,
    $schema:          'http://schemas.taskcluster.net/base/v1/' +
                        'exchanges-reference.json#',
    serviceName:        this._options.serviceName,
    title:              this._options.title,
    description:        this._options.description,
    exchangePrefix:     exchangePrefix,
    entries: this._entries.map(entry => {
      return {
        type:           'topic-exchange',
        exchange:       entry.exchange,
        name:           entry.name,
        title:          entry.title,
        description:    entry.description,
        routingKey:     entry.routingKey.map(function(key) {
          return _.pick(key, 'name', 'summary', 'constant',
            'multipleWords', 'required');
        }),
        schema: entry.schema,
      };
    }),
  };

  var ajv = Ajv({useDefaults: true, format: 'full', verbose: true, allErrors: true});
  // Load exchanges-reference.json schema from disk
  var schemaPath = path.join(__dirname, 'schemas', 'exchanges-reference.json');
  var schema = fs.readFileSync(schemaPath, {encoding: 'utf-8'});
  var validate = ajv.compile(JSON.parse(schema));

  // Check against it
  var refSchema = 'http://schemas.taskcluster.net/base/v1/' +
                  'exchanges-reference.json#';
  var valid = validate(reference, refSchema);
  if (!valid) {
    debug('Exchanges.references(): Failed to validate against schema, ' +
          'errors: %j reference: %j', validate.errors, reference);
    throw new Error('API.references(): Failed to validate against schema');
  }

  // Return reference
  return reference;
};

/**
 * Publish JSON reference for the declared exchanges
 *
 * options:
 * {
 *   rootUrl: ...,
 *   credentials: {
 *     username:        '...',                // Pulse username (optional)
 *   },
 *   aws: {             // AWS credentials and region
 *    accessKeyId:      '...',
 *    secretAccessKey:  '...',
 *    region:           'us-west-2'
 *   }
 * }
 *
 * Return a promise that reference was published.
 */
Exchanges.prototype.publish = function(options) {
  // Provide default options
  options = _.defaults({}, options || {}, this._options);
  assert(options.rootUrl, 'rootUrl is required');
  assert(!options.exchangePrefix, 'exchangePrefix is not allowed');
  assert(!options.referencePrefix, 'referencePrefix is not allowed');
  assert(!options.referenceBucket, 'referenceBucket is not allowed');
  assert(options.aws, 'aws must be provided');

  const refUrl = libUrls.exchangeReference(options.rootUrl, options.serviceName, options.version);
  const {hostname, path} = url.parse(refUrl);

  // Create S3 object
  var s3 = new aws.S3(options.aws);

  // Upload object
  return s3.putObject({
    Bucket:           hostname,
    Key:              path.slice(1), // omit leading `/`
    Body:             JSON.stringify(this.reference(), undefined, 2),
    ContentType:      'application/json',
  }).promise();
};

/**
 * Setup exchanges, return promise for a publisher and publish reference if,
 * ordered to do so.
 *
 * options:
 * {
 *   publish:        false // Publish reference during setup
 * }
 *
 * Takes the same options as `publish` and `connect`.
 */
Exchanges.prototype.setup = function(options) {
  var promises = [];
  promises.push(this.connect(options));
  if (options.publish === true) {
    promises.push(this.publish(options));
  }
  return Promise.all(promises).then(function(vals) {
    return vals[0]; // Return publisher
  });
};

// Export the Exchanges class
module.exports = Exchanges;

// Export reference to Publisher
Exchanges.Publisher = Publisher;
