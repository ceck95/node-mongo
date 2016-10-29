/*
 * @Author: toan.nguyen
 * @Date:   2016-04-19 15:15:27
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-10-17 13:35:20
 */

'use strict';

const Hoek = require('hoek');
const config = require('config');
const helpers = require('node-helpers');
const MongoError = require('mongodb').MongoError;

const isDebug = config.has('isDebug') ? config.get('isDebug') : (!process.env.NODE_ENV || process.env.NODE_ENV === 'development');

class BaseService {

  /**
   * Adapter class for current service
   *
   * @return {Object} Adapter object
   */
  get adapterClass() {
    Hoek.assert(false, 'adapterClass has not been implemented');
  }

  /**
   * Return exception handler
   *
   * @return {ExceptionHelper} Exception helpers
   */
  get exception() {
    return helpers.DefaultException;
  }

  /**
   * Constructor, set default data
   */
  constructor() {
    this.adapter = new this.adapterClass();
    this.model = new this.adapter.modelClass();
  }

  /**
   * Prints query log error
   *
   * @param  {Error} err     Error object
   * @param  {Function} result Result callback
   */
  catchException(err, result) {
    this.adapter.log.error(err);

    if (isDebug) {
      throw err;
    }

    return this.responseError(err, result);
  }


  /**
   * Response result default data
   *
   * @param  {Object} prom Adapter query promise
   * @param  {Object} result Result callback
   */
  responseDefault(prom, result) {
    let self = this;

    return prom.then(data => {
      return result(null, data);
    }).catch(MongoError, (err) => {
      return self.responseError(err, result);
    }).catch(e => {
      return self.catchException(e, result);
    });

  }

  /**
   * Response result with a instance model
   *
   * @param  {Object} prom Adapter query promise
   * @param  {Object} result Result callback
   */
  responseOne(prom, result) {
    let self = this;

    return prom.then(data => {
      let resultModel = data.toThriftObject ? data : new self.adapter.modelClass(data);
      return result(null, resultModel.toThriftObject());
    }).catch(MongoError, (err) => {
      return self.responseError(err, result);
    }).catch(e => {
      return self.catchException(e, result);
    });
  }

  /**
   * Response result with a instance model
   *
   * @param  {Object} prom Adapter query promise
   * @param  {Object} opts Option data
   * @param  {Object} result Result callback
   *
   * @return {Function}      Callback result
   */
  responseGetOne(prom, opts, result) {

    opts = opts || {};

    let self = this;

    return prom.then(data => {
      if (!data) {
        var ex1 = self.exception.create(helpers.Error.notFound(), {
          table: self.model.collectionName
        });

        return result(ex1);
      }

      var respModel = new self.adapter.modelClass(data);

      return result(null, respModel.toThriftObject());
    }).catch(MongoError, (err) => {
      return self.responseError(err, result);
    }).catch(e => {
      return self.catchException(e, result);
    });
  }

  /**
   * Response result with multiple model instances
   *
   * @param  {Object} prom Adapter query promise
   * @param  {Object} opts Option data
   * @param  {Object} result Result callback
   */
  responseMany(prom, opts, result) {
    opts = opts || {};

    let self = this;

    return prom.then(data => {
      let responseData = [];
      if (!data) {
        return result(null, responseData);
      }
      data.forEach((element) => {
        let item = new self.adapter.modelClass(element);
        responseData.push(item.toThriftObject());
      });

      return result(null, responseData);
    }).catch(MongoError, (err) => {
      return self.responseError(err, result);
    }).catch(e => {
      return self.catchException(e, result);
    });
  }

  /**
   * Response result with multiple model instances, with pagination
   *
   * @param  {Object} prom Adapter query promise
   * @param  {Object} opts Option data
   * @param  {Object} result Result callback
   */
  responsePagination(prom, opts, result) {

    let self = this;

    return prom.then(resp => {
      let response = new self.adapter.modelClass.paginationThriftClass();
      response.pagination = resp.meta;

      let responseData = [];

      resp.data.forEach((element) => {

        let tempModel = new self.adapter.modelClass(element);
        responseData.push(tempModel.toThriftObject());
      });

      response.data = responseData;
      return result(null, response);

    }).catch(MongoError, (err) => {
      return self.responseError(err, result);
    }).catch(e => {
      return self.catchException(e, result);
    });
  }

  /**
   * Response Error callback service
   *
   * @param  {Object} err     Error data
   * @param  {Function} result  Result callback
   */
  responseError(err, result) {
    this.adapter.log.error(err);

    let ex = this.exception.create(err, {
      type: 'mongo',
      table: this.model.collectionName
    });

    return result(ex);
  }

  /**
   * Insert model into database
   *
   * @param  {Object} form  Form data
   * @param  {Function} result Result callback
   */
  insertOne(form, result) {
    return this.responseOne(this.adapter.insertOne(form), result);
  }

  /**
   * Insert many models into database
   *
   * @param  {Array} models  Models data
   * @param  {Function} result Result callback
   */
  // insertMany(models, result) {

  //   var self = this;

  //   self.adapter.insertMany(models, {}, (err, data) => {
  //     return self.responseMany(self, err, data, {}, result);
  //   });
  // }


  /**
   * Update model into database
   *
   * @param  {Object} form  Form data
   * @param  {Object} params  Query data
   * @param  {Function} result Result callback
   */
  updateOne(form, params, result) {
    return this.responseDefault(this.adapter.updateOne(form, params), result);
  }

  /**
   * Update raw data into database
   *
   * @param  {Object} form  Form data
   * @param  {Object} params  Query data
   * @param  {Function} result Result callback
   */
  updateOneSimple(form, params, result) {
    return this.responseDefault(this.adapter.updateOneSimple(form, params), result);
  }

  /**
   * Update many models into database
   *
   * @param  {Object} form  Form data
   * @param  {Object} params  Query data
   * @param  {Function} result Result callback
   */
  updateMany(form, params, result) {
    return this.responseDefault(this.adapter.updateMany(form, params), result);
  }

  /**
   * Updates raw data into many collections
   *
   * @param  {Object} form  Form data
   * @param  {Object} params  Query data
   * @param  {Function} result Result callback
   */
  updateManySimple(form, params, result) {
    return this.responseDefault(this.adapter.updateManySimple(form, params), result);
  }

  /**
   * Update all models into database
   *
   * @param  {Object} form  Form data
   * @param  {Function} result Result callback
   */
  updateAll(form, result) {
    return this.responseDefault(this.adapter.updateAll(form), result);
  }

  /**
   * Updates raw data into many collections
   *
   * @param  {Object} form  Form data
   * @param  {Function} result Result callback
   */
  updateAllSimple(form, result) {
    return this.responseDefault(this.adapter.updateAllSimple(form), result);
  }

  /**
   * Upserts model into database
   *
   * @param  {Object} form  Form data
   * @param  {Function} result Result callback
   */
  upsertOne(form, result) {
    return this.responseOne(this.adapter.upsertOne(form), result);
  }

  /**
   * Get single object from database, return service
   *
   * @param  {object} uid     model uid
   * @param  {Function} result Result callback
   */
  getOne(form, opts, result) {
    opts = opts || {};

    if (typeof(opts) === 'function') {
      result = opts;
      opts = {};
    }
    return this.responseGetOne(this.adapter.getOne(form, opts), opts, result);
  }

  /**
   * Get one row with simple query
   *
   * @param  {Object} form Query form
   * @param  {Function} result Result callback
   */
  getOneSimple(form, opts, result) {
    opts = opts || {};

    if (typeof(opts) === 'function') {
      result = opts;
      opts = {};
    }
    return this.responseGetOne(this.adapter.getOneSimple(form), opts, result);
  }

  /**
   * Update model into database
   *
   * @param  {Object} form  Form data
   * @param  {Object} params  Query data
   * @param  {Function} result Result callback
   */
  getOneAndUpdate(form, params, opts, result) {
    return this.responseGetOne(this.adapter.getOneAndUpdate(form, params, opts), opts, result);
  }

  /**
   * Get one and upsert document from collection
   *
   * @param  {Object} form Update form
   * @param  {Object} query Query form
   * @param  {Object} opts Option data
   *
   * @return {Promise}      Query promise
   */
  getOneAndUpsert(form, query, opts, result) {
    return this.responseGetOne(this.adapter.getOneAndUpsert(form, query, opts), opts, result);
  }

  /**
   * Get many objects from database, return service
   *
   * @param  {Object} form Query form
   * @param  {Function} result Result callback
   */
  getMany(form, opts, result) {
    opts = opts || {};

    if (typeof(opts) === 'function') {
      result = opts;
      opts = {};
    }
    return this.responseMany(this.adapter.getMany(form, opts), opts, result);
  }

  /**
   * Get many objects from database, return service
   *
   * @param  {Object} form Query form
   * @param  {Function} result Result callback
   */
  getAllCondition(form, opts, result) {
    return this.getMany(form, opts, result);
  }

  /**
   * Get single object from database, return service
   *
   * @param  {Object} form Query form
   * @param  {Function} result Returned service data
   */
  exists(form, result) {
    return this.responseDefault(this.adapter.exists(form), result);
  }

  /**
   * Delete one record by primary key
   *
   * @param  {mixed} uid    Unique Primary key value
   * @param  {Function} result Callback result
   */
  deleteOne(form, result) {
    return this.responseDefault(this.adapter.deleteOne(form), result);
  }

  /**
   * Delete one record by primary key
   *
   * @param  {mixed} uid    Unique Primary key value
   * @param  {Function} result Callback result
   */
  deleteOneSimple(form, result) {
    return this.responseDefault(this.adapter.deleteOneSimple(form), result);
  }

  /**
   * Deletes many records by primary key
   *
   * @param  {Object} form    Query object data
   * @param  {Function} result Callback result
   */
  deleteMany(form, result) {
    return this.responseDefault(this.adapter.deleteMany(form), result);
  }

  /**
   * Deletes many documents from database
   *
   * @param  {Object} form Input form params
   *
   * @return {Promise}      Deleted promise
   */
  deleteManySimple(form, result) {
    return this.responseDefault(this.adapter.deleteManySimple(form), result);
  }

  /**
   * Deletes all documents from database
   *
   * @param  {Object} form Input form params
   *
   * @return {Promise}      Deleted promise
   */
  deleteAll(result) {
    return this.responseDefault(this.adapter.deleteAll(), result);
  }
}

module.exports = BaseService;
