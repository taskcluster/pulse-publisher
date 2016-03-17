# Pulse Publisher

[![Build Status](https://travis-ci.org/taskcluster/pulse-publisher.svg?branch=master)](https://travis-ci.org/taskcluster/pulse-publisher)

A collection of utilities for interacting with Mozilla's [Pulse](https://pulseguardian.mozilla.org/).

## Requirements

This is tested on and should run on any of node `{0.12, 4, 5}`.

## Testing
You'll need to fill a file called `taskcluster-base-test.conf.json` with valid keys. There is a `taskcluster-base-test-example.conf.json` you can copy over to see which keys are needed. Then it is just a matter of `npm install` and `npm test`.

## License
[Mozilla Public License Version 2.0](https://github.com/taskcluster/pulse-publisher/blob/master/LICENSE)
