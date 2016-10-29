/*
 * @Author: toan.nguyen
 * @Date:   2016-09-04 16:36:19
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-09-09 18:40:54
 */

'use strict';

const BaseService = require('../base/service');

class UserActivityService extends BaseService {

  /**
   * Query driver around
   *
   * @param  {Object} params  Request params
   * @param  {Object} actParams  Activity params
   * @param  {Function} result Callback function
   *
   */
  findManyAround(params, actParams, result) {

    let opts = {};

    return this.responseMany(this.adapter.findManyAround(params, actParams), opts, result);
  }

  /**
   * Query one driver around
   *
   * @param  {Object} params  Request params
   * @param  {Function} result Callback function
   *
   */
  findOneAround(params, actParams, result) {
    return this.responseOne(this.adapter.findOneAround(params, actParams), result);
  }
}

module.exports = UserActivityService;
