var assert        = require('assert');
var debug         = require('debug')('base:exchanges');
var _             = require('lodash');
var Promise       = require('promise');
var events        = require('events');
var util          = require('util');
var common        = require('./common');

var FakePublisher = function(entries, exchangePrefix, options) {
  events.EventEmitter.call(this);
  assert(options.validator, 'options.validator must be provided');
  this._entries = entries;
  this._options = options;
  this._closing = false;
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

  var that = this;
  entries.forEach(function(entry) {
    that[entry.name] = function() {
      // Copy arguments
      var args = Array.prototype.slice.call(arguments);

      // Construct message and routing key from arguments
      var message = entry.messageBuilder.apply(undefined, args);
      common.validateMessage(that._options.validator, entry, message);

      var routingKey = common.routingKeyToString(entry, entry.routingKeyBuilder.apply(undefined, args));

      var CCs = entry.CCBuilder.apply(undefined, args);
      assert(CCs instanceof Array, 'CCBuilder must return an array');

      var payload = _.cloneDeep(message);

      // Find exchange name
      var exchange = exchangePrefix + entry.exchange;

      // Log that we're publishing a message
      debug('Faking publish of message on exchange: %s', exchange);

      that.emit('fakePublish', {exchange, routingKey, payload, CCs});

      // Return promise
      return Promise.resolve();
    };
  });
};

// Inherit from events.EventEmitter
util.inherits(FakePublisher, events.EventEmitter);

/** Close the connection */
FakePublisher.prototype.close = function() {};

module.exports = FakePublisher;
