# Pulse Publisher

[![Build Status](https://travis-ci.org/taskcluster/pulse-publisher.svg?branch=master)](https://travis-ci.org/taskcluster/pulse-publisher)

A collection of utilities for interacting with Mozilla's [Pulse](https://pulseguardian.mozilla.org/).

## Requirements

This is tested on and should run on any of node `{7, 8, 10}`.

## Usage
The source-code contains additional comments for each method.

```js
// Exchanges are typically declared in a file like exchanges.js
let Exchanges = require('pulse-publisher');

// Create a new set of exchanges
let exchanges = new Exchanges({
  title: 'Title for Exchanges Docs',
  description: [
    'Description in **markdown**.',
    'This will available in reference JSON',
  ].join(''),
  schemaPrefix: 'https://.../', // Prefix for all schema keys given in exchanges.declare
});

// Declare an exchange
exchanges.declare({
  exchange: 'test-exchange', // Name-suffix for exchange on RabbitMQ
  name:     'testExchange',  // Name of exchange in reference JSON (used client libraries)
  title:    'Title for Exchange Docs',
  description: [
    'Description in **markdown**.',
    'This will available in reference JSON',
  ].join(''),
  schema:   'name-of-my-schema.json',   // Schema for payload, prefix by schemaPrefix
  messageBuilder: (message) => message, // Build message from arguments given to publisher.testExchange(...)
  routingKey: [
    // Declaration of elements that makes up the routing key
    {
      // First element should always be constant 'primary' to be able to identify
      // primary routingkey from CC'ed routing keys.
      name: 'routingKeyKind', // Identifier used in client libraries
      summary: 'Routing key kind hardcoded to primary in primary routing-key',
      constant: 'primary',
      required: true,
    }, {
      name: 'someId', // See routingKeyBuilder
      summary: 'Brief docs',
      required: true || false, // If false, the default value is '_'
      maxSize: 22,    // Max size is validated when sending
      multipleWords: true || false, // If true, the value can contain dots '.'
    }, {
      // Last element should always be multiple words (and labeled reserved)
      // The only way to match it is with a #, hence, we ensure that clients are
      // compatible with future routing-keys if add addtional entries in the future.
      name:             'reserved',
      summary:          'Space reserved for future use.',
      multipleWords:    true,
      maxSize:          1,
    }
  ],
  routingKeyBuilder: (message) => {
    // Build routingKey from arguments given to publisher.testExchange(...)
    // This can return either a string or an object with a key for each
    // required property specified in 'routingKey' above.
    return {
      someId: message.someIdentifier,
    };
  },
  CCBuilder: (message) => {
    // Construct CC'ed routingkeys as strings from arguments given to publisher.testExchanges(...)
    // By convention they should all be prefixed 'route.', so they don't interfer with the primary
    // routing key.
    return message.routes.map(r => 'route.' + r);
  },
});

// Note you can declare multiple exchanges, by calling exchanges.declare again.
// Nothing happens on AMQP before exchanges.connect() is called. This just declares
// the code in JS.

// Some where in your app, instantiate a publisher
let publisher = await exchanges.connect({
  credentials: {clientId: '...', accessToken: '...'}, // client that can use tc-pulse
  namespace: '...', // namespace for the pulse exchanges (e.g., `taskcluster-glurble`)
  expires: '1 day', // lifetime of the namespace
  exchangePrefix: 'v1/', // Prefix for all exchanges (in addition to exchanges/<namespace>/)
  validator: await require('taskcluster-lib-validate'), // instance of taskcluster-lib-validate
  monitor: undefined, // optional instance of taskcluster-lib-monitor
});

// Send a message to the declared testExchange
await publisher.testExchange({someIdentifier: '...', routes: [], ...});
```

Alternately, if using
[taskcluster-lib-loader](https://github.com/taskcluster/taskcluster-lib-loader/pull/17/files),
create a loader component that calls `setup`, which will also publish the exchange reference:

```js
  publisher: {
    requires: ['cfg', 'validator', 'monitor'],
    setup: ({cfg, validator, monitor}) => exchanges.setup({
      credentials:        cfg.app.taskcluster,
      namespace:          cfg.pulse.namespace,
      expires:            cfg.pulse.expires,
      exchangePrefix:     cfg.app.exchangePrefix,
      validator:          validator,
      referencePrefix:    'myservice/v1/exchanges.json',
      publish:            cfg.app.publishMetaData,
      aws:                cfg.aws,
      monitor:            monitor.prefix('publisher'),
    }),
  },
```

Docs can also be generated with exchange.reference(). See the source code docs for details.

### Test Support

For testing, it is useful to be able to verify that messages were sent without
requiring a real AMQP server.

Pass `credentials: {fake: true}` to `connect` or `setup` to get this behavior.
No AMQP connection will be made, but each call to a message-publishing method
will result in a "fakePublish" message emitted from the publisher with content
`{exchange, routingKey, payload, CCs}`.

## Testing
You'll need to fill a file called `user-config.yml` with valid keys. There is a `user-config-exaemple.yml` you can copy over to see which keys are needed. Then it is just a matter of `yarn install` and `yarn test`.

## License
[Mozilla Public License Version 2.0](https://github.com/taskcluster/pulse-publisher/blob/master/LICENSE)
