/*
 * @Author: toan.nguyen
 * @Date:   2016-07-14 19:51:55
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-10-17 13:34:22
 */

'use strict';

const config = require('config');
const Hoek = require('hoek');
const BPromise = require('bluebird');
const helpers = require('node-helpers');

const pool = require('./pool');
const MongoHelpers = require('./helpers');
const MongoError = require('mongodb').MongoError;
const ObjectID = require('mongodb').ObjectID;

const isDebug = config.has('isDebug') ? config.get('isDebug') : (!process.env.NODE_ENV || process.env.NODE_ENV === 'development');

class BaseAdapter {

  /**
   * Constructor, set default values
   */
  constructor() {
    this.model = new this.modelClass();
    this.collectionName = this.model.collectionName;
  }

  /**
   * Default Log namespace
   *
   * @return {String}
   */
  get logNamespace() {
    return 'mongo';
  }

  /**
   * Returns model class for current adapter
   *
   * @return {Object} Model class
   */
  get modelClass() {
    Hoek.assert(false, 'modelClass method has not been implemented');
  }

  /**
   * Return mongoDB config key
   *
   * @return {String}
   */
  get configKey() {
    return 'default';
  }

  /**
   * Prints query log error
   *
   * @param  {Error} err     Error object
   * @param  {String} message Error message
   * @param  {Object} data    Query data
   * @param  {Object} query    Query object
   * @param  {Object} options Query options
   */
  catchException(err, message, data, query, options) {
    this.log.error(err, message, data, query, options);

    if (isDebug) {
      throw err;
    }

  }

  /**
   * Connects to mongodb, then run query
   *
   * @param  {String} name Connection config
   *
   * @return {Promise}           Promise result
   */
  connect(key) {
    key = key || this.configKey;
    return pool.connect(key);
  }

  /**
   * Run query, catch exceptions
   *
   * @param  {String} funcName  Mongo query function name
   * @param {Object} requestDoc Request document data
   * @param {Object} query Object
   *
   * @return {Promise}            Mongo query promise
   */
  query(funcName, ...args) {

    Hoek.assert(funcName, 'funcName must not be null');

    let self = this,
      message = funcName + ' on collection ' + this.collectionName + ' failed';

    return new BPromise((resolve, reject) => {
      return self.connect().then(db => {

        let collection = db.collection(self.collectionName);

        self.log.debug(funcName + ' ' + self.collectionName + '. Request data:', ...args);

        return collection[funcName](...args).then(result => {
          return resolve(result);
        }).catch(MongoError, e => {
          self.log.error(message, e, ...args);
          return reject(e);
        }).catch(e => {
          self.catchException(e, message, ...args);
          return reject(e);
        });
      });
    });

  }

  /**
   * Inserts document into database
   *
   * @param  {Object} model Input model data
   *
   * @return {Promise}      Inserts promise
   */
  insertOne(model) {

    let self = this,
      insertModel = new self.modelClass(model);

    if (insertModel.beforeSave) {
      insertModel.beforeSave(true);
    }

    let requestDoc = insertModel.toInsertObject();

    Hoek.assert(!helpers.Data.isEmpty(requestDoc), 'Request document must not be empty');

    return self.query('insertOne', requestDoc).then(result => {
      self.log.debug('Insert ' + self.collectionName + ' successfully. ID: ', result.insertedId);
      insertModel._id = result.insertedId;

      return BPromise.resolve(insertModel);
    });

  }

  /**
   * Updates document into database
   *
   * @param  {Object} model Input model data
   *
   * @return {Promise}      Inserts promise
   */
  updateOne(model, params) {

    let self = this,
      updateModel = new self.modelClass(model),
      collectionName = updateModel.collectionName,
      queryParams = updateModel.toQueryObject ? updateModel.toQueryObject(params) : helpers.Model.toSimpleObject(params);

    if (updateModel.beforeSave) {
      updateModel.beforeSave(false);
    }

    let requestDoc = updateModel.toFormObject();

    Hoek.assert(!helpers.Data.isEmpty(queryParams), 'Query params must not be empty. Data: ' + JSON.stringify(params));
    Hoek.assert(!helpers.Data.isEmpty(requestDoc), 'Request document must not be empty');

    return self.query('updateOne', queryParams, {
      $set: requestDoc
    }).then(result => {
      self.log.debug('Update ' + collectionName + ' successfully. Modified count: ', result.modifiedCount);

      return BPromise.resolve(result.modifiedCount);
    });

  }



  /**
   * Updates params into database
   *
   * @param  {Object} form Input update data
   * @param  {Object} query Query params
   *
   * @return {Promise}      Update promise
   */
  updateOneSimple(form, query) {

    let self = this,
      params = helpers.Model.toSimpleObject(form),
      queryParams = helpers.Model.toSimpleObject(query);

    Hoek.assert(query, 'Empty query, cannot update data');

    return self.query('updateOne', queryParams, {
      '$set': params
    }).then(result => {
      self.log.debug('Updated successfully', result.modifiedCount);

      return BPromise.resolve(result.modifiedCount);
    });
  }

  /**
   * Updates many documents into database
   *
   * @param  {Object} model Input model data
   *
   * @return {Promise}      Updates promise
   */
  updateMany(model, params) {

    let self = this,
      updateModel = new self.modelClass(model),
      collectionName = updateModel.collectionName,
      queryParams = updateModel.toQueryObject ? updateModel.toQueryObject(params) : helpers.Model.toSimpleObject(params);

    if (updateModel.beforeSave) {
      updateModel.beforeSave(false);
    }

    let requestDoc = updateModel.toFormObject();

    Hoek.assert(!helpers.Data.isEmpty(queryParams), 'Query parms must not be empty');
    Hoek.assert(!helpers.Data.isEmpty(requestDoc), 'Request document must not be empty');

    return self.query('updateMany', queryParams, {
      $set: requestDoc
    }).then(result => {
      self.log.debug('updateMany ' + collectionName + ' successfully. Modified count: ', result.modifiedCount);

      return BPromise.resolve(result.modifiedCount);
    });
  }

  /**
   * Updates all documents into database
   *
   * @param  {Object} model Input model data
   *
   * @return {Promise}      Updates promise
   */
  updateAll(model) {

    let self = this,
      updateModel = new self.modelClass(model),
      collectionName = updateModel.collectionName,
      queryParams = {};

    if (updateModel.beforeSave) {
      updateModel.beforeSave(false);
    }

    let requestDoc = updateModel.toFormObject();

    Hoek.assert(!helpers.Data.isEmpty(requestDoc), 'Request document must not be empty');

    return self.query('updateMany', queryParams, {
      $set: requestDoc
    }).then(result => {
      self.log.debug('Update ' + collectionName + ' successfully. Modified count: ', result.modifiedCount);

      return BPromise.resolve(result.modifiedCount);
    });
  }

  /**
   * Updates simple data to all documents into database
   *
   * @param  {Object} model Input model data
   *
   * @return {Promise}      Updates promise
   */
  updateAllSimple(params) {

    let self = this,
      updateModel = new self.modelClass(),
      collectionName = updateModel.collectionName,
      queryParams = {},
      requestDoc = helpers.Model.toSimpleObject(params);

    Hoek.assert(!helpers.Data.isEmpty(requestDoc), 'Request document must not be empty');

    return self.query('updateMany', queryParams, {
      $set: requestDoc
    }).then(result => {
      self.log.debug('Update ' + collectionName + ' successfully. Modified count: ', result.modifiedCount);

      return BPromise.resolve(result.modifiedCount);
    });
  }

  /**
   * Updates many params into database
   *
   * @param  {Object} form Input update data
   * @param  {Object} query Query params
   *
   * @return {Promise}      Update promise
   */
  updateManySimple(form, query) {

    let self = this,
      params = helpers.Model.toSimpleObject(form),
      queryParams = helpers.Model.toSimpleObject(query);

    Hoek.assert(!helpers.Data.isEmpty(query), 'Query params must not be empty');

    return self.query('updateMany', queryParams, {
      '$set': params
    }).then(result => {
      self.log.debug('Updated successfully', result.modifiedCount);

      return BPromise.resolve(result.modifiedCount);
    });
  }

  /**
   * Upserts document into database
   *
   * @param  {Object} model Input model data
   *
   * @return {Promise}      Inserts promise
   */
  upsertOne(model) {

    let self = this,
      updateModel = new self.modelClass(model);

    if (updateModel.beforeSave) {
      updateModel.beforeSave(true);
    }

    let requestDoc = updateModel.toUpsertObject(),
      queryParams = requestDoc.$setOnInsert;

    Hoek.assert(!helpers.Data.isEmpty(queryParams), 'Query params must not be empty');
    Hoek.assert(!helpers.Data.isEmpty(requestDoc), 'Request document must not be empty');

    return self.query('updateOne', queryParams, requestDoc, {
      upsert: true
    }).then(result => {
      self.log.debug('Upsert ' + self.collectionName + ' successfully. Upserted count: ', result.upsertedCount);

      if (result.upsertedId) {
        updateModel._id = result.upsertedId._id.toString();
      }
      return BPromise.resolve(updateModel);
    });
  }

  /**
   * Get session document by user
   *
   * @param  {Object} form Query form
   *
   * @return {Promise}      Query promise
   */
  getOne(form, opts) {

    let self = this,
      model = new self.modelClass(),
      params = null;

    if (form._id) {
      params = {
        _id: new ObjectID(form._id)
      };
    } else if (model.toQueryObject) {
      params = model.toQueryObject(form);
    } else {
      params = helpers.Model.toSimpleObject(form);
    }

    opts = MongoHelpers.buildFindOptions(model, opts);

    return self.query('findOne', params, opts).then(result => {
      self.log.debug('GetOne successfully:', !!result);
      return BPromise.resolve(result);
    });
  }

  /**
   * Get one row with simple query
   *
   * @param  {Object} form Query form
   *
   * @return {Promise}      Query promise
   */
  getOneSimple(form) {

    let self = this,
      params = helpers.Model.toSimpleObject(form);

    Hoek.assert(!helpers.Data.isEmpty(params), 'Params must not be empty');

    return self.query('findOne', params).then(result => {
      self.log.debug('getOneSimple successfully', !!result);
      return BPromise.resolve(result);
    });

  }

  /**
   * Get many sessions document by user
   *
   * @param  {Object} form Query form
   *
   * @return {Promise}      Query promise
   */
  getMany(form, opts) {
    opts = opts || {};

    let self = this,
      model = new self.modelClass(),
      collectionName = model.collectionName;

    return new BPromise((resolve, reject) => {

      return self.connect().then(db => {

        let collection = db.collection(collectionName),
          params = null;

        if (model.toQueryObject) {
          params = model.toQueryObject(form);
        } else {
          params = helpers.Model.toSimpleObject(form);

        }

        Hoek.assert(!helpers.Data.isEmpty(params), 'Params must not be empty');

        self.log.debug('Get many ' + collectionName + ' record', params, '. Options:', opts);

        let cursor = collection.find(params);

        let sortOptions = MongoHelpers.buildSortOptions(model, opts);
        if (sortOptions) {
          cursor = cursor.sort(sortOptions);
        }

        if (opts.skip) {
          cursor = cursor.skip(opts.skip);
        }

        if (opts.limit) {
          cursor = cursor.limit(opts.limit);
        }

        return cursor.toArray().then((docs) => {
          self.log.error('getMany ' + collectionName + ' successfully. Count:', docs.length);
          return resolve(docs);
        }).catch(MongoError, e => {
          self.log.error('GetMany ' + collectionName + ' failed.', e, form);
          return reject(e);
        }).catch(e => {
          self.catchException(e, 'GetMany ' + collectionName + ' failed.', form);
          return reject(e);
        });
      });
    });
  }

  /**
   * Get many sessions document by user
   *
   * @param  {Object} form Query form
   *
   * @return {Promise}      Query promise
   */
  getAllCondition(form, opts) {
    return this.getMany(form, opts);
  }

  /**
   * Get one and update document from collection
   *
   * @param  {Object} query Query form
   * @param  {Object} form Update form
   * @param  {Object} opts Option data
   *
   * @return {Promise}      Query promise
   */
  getOneAndUpdate(form, query, opts) {

    let self = this,
      updateModel = new self.modelClass(form),
      collectionName = updateModel.collectionName,
      queryParams = updateModel.toQueryObject ? updateModel.toQueryObject(query) : helpers.Model.toSimpleObject(query);

    if (updateModel.beforeSave) {
      updateModel.beforeSave(false);
    }

    let requestDoc = updateModel.toFormObject();

    Hoek.assert(!helpers.Data.isEmpty(queryParams), 'Query params must not be empty. Data: ' + JSON.stringify(queryParams));
    Hoek.assert(!helpers.Data.isEmpty(requestDoc), 'Request document must not be empty');

    return self.query('findOneAndUpdate', queryParams, {
      $set: requestDoc
    }, opts).then(result => {
      self.log.debug('findOneAndUpdate ' + collectionName + ' successfully. Modified: ', result);
      if (!result.ok) {
        return BPromise.resolve(null);
      }

      return BPromise.resolve(result.value);
    });

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
  getOneAndUpsert(form, query, opts) {

    let self = this,
      updateModel = new self.modelClass(form),
      collectionName = updateModel.collectionName,
      queryParams = updateModel.toQueryObject ? updateModel.toQueryObject(query) : helpers.Model.toSimpleObject(query);

    if (updateModel.beforeSave) {
      updateModel.beforeSave(false);
    }

    let requestDoc = updateModel.toUpsertObject();
    opts.upsert = true;

    Hoek.assert(!helpers.Data.isEmpty(queryParams), 'Query params must not be empty. Data: ' + JSON.stringify(queryParams));
    Hoek.assert(!helpers.Data.isEmpty(requestDoc), 'Request document must not be empty');

    return self.query('findOneAndUpdate', queryParams, requestDoc, opts).then(result => {
      self.log.debug('findOneAndUpdate ' + collectionName + ' successfully. Modified: ', result);
      if (!result.ok) {
        return BPromise.resolve(null);
      }

      return BPromise.resolve(result.value);
    });

  }


  /**
   * Checks if document is existed
   *
   * @param  {Object} form Query form
   *
   * @return {Promise}      Query promise
   */
  exists(form) {

    var self = this,
      model = new self.modelClass(),
      collectionName = model.collectionName;

    return new BPromise((resolve, reject) => {

      return self.connect().then(db => {

        let collection = db.collection(collectionName),
          params = null;

        if (form._id) {
          params = {
            _id: new ObjectID(form._id)
          };
        } else if (model.toQueryObject) {
          params = model.toQueryObject(form);
        } else {
          params = helpers.Model.toSimpleObject(form);
        }

        Hoek.assert(!helpers.Data.isEmpty(params), 'Params must not be empty');

        self.log.debug('Exists ' + collectionName + ' record', params);
        return collection.find(params).limit(1).count().then((count) => {
          return resolve(count > 0);
        }).catch(MongoError, e => {
          self.log.error('Exists ' + collectionName + ' failed.', e, form);
          return reject(e);
        }).catch(e => {
          self.catchException(e, 'Exists ' + collectionName + ' failed.', form);
          return reject(e);
        });
      });
    });
  }

  /**
   * Deletes 1 document into database
   *
   * @param  {Object} model Input model data
   *
   * @return {Promise}      Inserts promise
   */
  deleteOne(form) {

    let self = this,
      params = {},
      model = new self.modelClass(),
      collectionName = model.collectionName;

    if (form._id) {
      params = {
        _id: new ObjectID(form._id)
      };
    } else if (model.toQueryObject) {
      params = model.toQueryObject(form);
    } else {
      params = helpers.Model.toSimpleObject(form);

    }

    Hoek.assert(!helpers.Data.isEmpty(params), 'Params must not be empty');
    return self.query('deleteOne', params).then(result => {
      self.log.debug('DeletedOne ' + collectionName + ' successfully. Count', result.deletedCount);

      return BPromise.resolve(result.deletedCount);
    });
  }

  /**
   * Deletes 1 document into database, with simple object query
   *
   * @param  {Object} model Input model data
   *
   * @return {Promise}      Inserts promise
   */
  deleteOneSimple(form) {

    let self = this,
      params = helpers.Model.toSimpleObject(form);

    Hoek.assert(!helpers.Data.isEmpty(params), 'Params must not be empty');

    return self.query('deleteOne', params).then(result => {
      self.log.debug('deleteOneSimple ' + self.collectionName + ' successfully. Count', result.deletedCount);

      return BPromise.resolve(result.deletedCount);
    });
  }

  /**
   * Deletes many documents from database
   *
   * @param  {Object} form Input form params
   *
   * @return {Promise}      Deleted promise
   */
  deleteMany(form) {

    let self = this,
      params = {},
      model = new self.modelClass(),
      collectionName = model.collectionName;

    if (model.toQueryObject) {
      params = model.toQueryObject(form);
    } else {
      params = helpers.Model.toSimpleObject(form);
    }

    Hoek.assert(!helpers.Data.isEmpty(params), 'Params must not be empty');

    return self.query('deleteMany', params).then(result => {
      self.log.debug('DeleteMany ' + collectionName + ' successfully. Count', result.deletedCount);

      return BPromise.resolve(result.deletedCount);
    });
  }

  /**
   * Deletes many documents from database
   *
   * @param  {Object} form Input form params
   *
   * @return {Promise}      Deleted promise
   */
  deleteManySimple(form) {

    let self = this,
      params = helpers.Model.toSimpleObject(form);

    Hoek.assert(!helpers.Data.isEmpty(params), 'Params must not be empty');

    return self.query('deleteMany', params).then(result => {
      self.log.debug('DeleteMany ' + this.collectionName + ' successfully. Count', result.deletedCount);

      return BPromise.resolve(result.deletedCount);
    });
  }

  /**
   * Deletes all documents from database
   *
   * @param  {Object} form Input form params
   *
   * @return {Promise}      Deleted promise
   */
  deleteAll() {

    let self = this,
      params = {};

    return self.query('deleteMany', params).then(result => {
      self.log.debug('DeleteAll ' + self.collectionName + ' successfully. Count', result.deletedCount);

      return BPromise.resolve(result.deletedCount);
    });
  }
}

module.exports = BaseAdapter;
