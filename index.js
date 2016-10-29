/*
 * @Author: toan.nguyen
 * @Date:   2016-06-06 18:55:41
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-09-04 16:38:59
 */

'use strict';

module.exports = {
  pool: require('./base/pool'),
  adapters: {
    Base: require('./base/adapter'),
    UserActivity: require('./adapters/user-activity')
  },
  services: {
    Base: require('./base/service'),
    UserActivity: require('./services/user-activity')
  }
};
