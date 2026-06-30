'use strict';

const app = require('../server');

module.exports = function handler(req, res) {
  return app(req, res);
};
