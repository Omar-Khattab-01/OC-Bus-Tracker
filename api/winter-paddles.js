'use strict';

const app = require('../server');

module.exports = function handler(req, res) {
  req.url = '/winter-paddles';
  return app(req, res);
};
