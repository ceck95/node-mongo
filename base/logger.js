/*
 * @Author: toan.nguyen
 * @Date:   2016-07-14 20:17:56
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-07-14 20:18:42
 */

'use strict';

const bunyan = require('bunyan');

var logger = bunyan.createLogger({
  name: 'postgres',
  streams: [{
    stream: process.stdout,
    level: 'info'
  }]
});

module.exports = logger;
