/*
 * @Author: toan.nguyen
 * @Date:   2016-09-05 18:20:00
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-09-05 18:58:42
 */

'use strict';

class MongoHelpers {

  /**
   * Builds find options from model
   *
   * @param  {MongoModel} model Input mongo model
   * @param  {Object} opts  Option data
   *
   * @return {Object}       Applied find options
   */
  static buildFindOptions(model, opts) {

    opts = opts || {};

    opts.sort = MongoHelpers.buildSortOptions(model, opts);

    return opts;
  }


  /**
   * Builds sort options from model
   *
   * @param  {MongoModel} model Input mongo model
   * @param  {Object} opts  Option data
   * @return {Object}       Applied sort options
   */
  static buildSortOptions(model, opts) {

    opts = opts || {};

    if (opts.order) {
      let sortOptions = MongoHelpers.buildOrder(opts.order);
      delete opts.order;
      return sortOptions;
    } else if (opts.sort) {
      return MongoHelpers.buildOrder(opts.sort);
    } else if (model.defaultOrder) {
      return MongoHelpers.buildOrder(model.defaultOrder);
    }

    return null;
  }

  /**
   * Generates Query order from input order
   *
   * @param  {mixed}  order       Order data
   *
   * @return {Object}             Order object
   */
  static buildOrder(order) {

    if (!order) {
      return null;
    }

    let buildArrayFunc = (arrs) => {
      let results = {};
      arrs.forEach((element) => {
        element = element.trim();
        if (element.substr(0, 1) === '-') {
          results[element.substr(1)] = -1;
        } else {
          results[element] = 1;
        }
      });

      return results;
    };

    if (Array.isArray(order)) {
      return buildArrayFunc(order);
    }

    switch (typeof(order)) {
      case 'object':
        return order;
      case 'string':
        let oParts = order.split(',');
        return buildArrayFunc(oParts);
    }
  }
}

module.exports = MongoHelpers;
