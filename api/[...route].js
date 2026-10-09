'use strict';
const relay = require('../server.js');
module.exports = (req, res) => {
  req.url = (req.url || '/').replace(/^\/api(?=\/|\?|$)/, '') || '/';
  return relay(req, res);
};
module.exports.config = { api: { bodyParser: false } };
