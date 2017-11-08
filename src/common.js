
var assert        = require('assert');
var debug         = require('debug')('base:exchanges');

exports.validateMessage = function(validator, entry, message) {
  var err = validator(message, entry.schema);
  if (err) {
    debug('Failed validate message: %j against schema: %s, error: %j',
      message, entry.schema, err);
    throw new Error('Message validation failed. ' + err);
  }
};

exports.routingKeyToString = function(entry, routingKey) {
  if (typeof routingKey !== 'string') {
    routingKey = entry.routingKey.map(function(key) {
      var word = routingKey[key.name];
      if (key.constant) {
        word = key.constant;
      }
      if (!key.required && (word === undefined || word === null)) {
        word = '_';
      }
      // Convert numbers to strings
      if (typeof word === 'number') {
        word = '' + word;
      }
      assert(typeof word === 'string', 'non-string routingKey entry: '
          + key.name + ': ' + word);
      assert(word.length <= key.maxSize,
        'routingKey word: \'' + word + '\' for \'' + key.name +
          '\' is longer than maxSize: ' + key.maxSize);
      if (!key.multipleWords) {
        assert(word.indexOf('.') === -1, 'routingKey for ' + key.name +
            ' is not declared multipleWords and cannot contain \'.\' ' +
            'as is the case with \'' + word + '\'');
      }
      return word;
    }).join('.');
  }

  // Ensure the routing key is a string
  assert(typeof routingKey === 'string', 'routingKey must be a string');
  return routingKey;
};
