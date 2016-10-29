/*
 * @Author: toan.nguyen
 * @Date:   2016-09-04 16:16:37
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-09-13 18:29:46
 */

'use strict';

const Hoek = require('hoek');
const BPromise = require('bluebird');
const BaseAdapter = require('../base/adapter');

class UserActivityAdapter extends BaseAdapter {

  /**
   * Returns activity log adapter
   *
   * @return {Adapter}
   */
  get logAdapterClass() {
    Hoek.assert(false, 'logAdapterClass has not been implemented');
  }

  /**
   * Returns primary key for group find around
   *
   * @return {String}
   */
  get primaryKey() {
    Hoek.assert(false, 'primaryKey has not been implemented');
  }

  /**
   * Inserts document into database
   *
   * @param  {Object} model Input model data
   *
   * @return {Promise}      Inserts promise
   */
  upsertOne(model) {

    let self = this,
      logAdapter = new this.logAdapterClass(),
      logModel = new logAdapter.modelClass(model),
      insertModel = new this.modelClass(model);

    return self.connect().then(db => {

      if (logModel.beforeSave) {
        logModel.beforeSave(true);
      }

      if (insertModel.beforeSave) {
        insertModel.beforeSave(true);
      }

      let collection = db.collection(insertModel.collectionName),
        requestLogDoc = logModel.toInsertObject(),
        requestDoc = insertModel.toUpsertObject();

      // insert activity log
      logAdapter.query('insertOne', requestLogDoc).then(result => {
        self.log.debug('Insert ' + logModel.collectionName + ' model successfully', result.insertedId);
      });

      return collection.updateOne(requestDoc.$setOnInsert, requestDoc, { upsert: true }).then((result) => {

        collection.createIndex({ location: "2dsphere" }).then(indexName => {
          self.log.info('Create index successfully: ', indexName);
        });

        self.log.debug('Upsert ' + insertModel.collectionName + ' model successfully', result.upsertedId);

        insertModel._id = result.upsertedId;

        return BPromise.resolve(insertModel);
      });
    });

  }

  /**
   * Builds find around query
   *
   * @param  {geoTypes.GeoQuery} geoParams Geography query params
   * @param  {driverTypes.DriverActivtyQuery} actParams Driver activity params
   *
   * @return {Object}          MongoDB query
   */
  findAroundQuery(geoParams, actParams, opts) {

    let query = {};
    if (actParams.activity !== null) {
      query.activity = actParams.activity;
    }

    let queryData = [{
      $geoNear: {
        near: geoParams.geometry,
        maxDistance: geoParams.maxDistance || undefined,
        minDistance: geoParams.minDistance || undefined,
        distanceField: 'distance',
        query: query,
        spherical: true
      }
    }, {
      $sort: {
        distance: -1,
        status: -1
      }
    }, {
      $group: {
        _id: '$' + this.primaryKey,
        activity: { $first: '$$ROOT' }
      }
    }];

    this.log.debug('Selects geo data: ', JSON.stringify(queryData));

    return queryData;
  }

  /**
   * Search documents near specific location
   *
   * @param  {Object} location Location object
   *
   * @return {Array}           List of available shippers nearby
   */
  findManyAround(geoParams, actParams, opts) {

    let self = this,
      model = new self.modelClass(),
      collectionName = model.collectionName;

    return self.connect().then(db => {

      let collection = db.collection(collectionName),
        queryData = self.findAroundQuery.call(self, geoParams, actParams);

      return collection.aggregate(queryData).toArray().then(results => {

        self.log.debug('Selects around driver successfully. Count: ', results.length);

        var response = [];

        results.forEach((element) => {
          response.push(element.activity);
        });
        return BPromise.resolve(response);
      });
    });

  }

  /**
   * Search documents near specific location
   *
   * @param  {Object} location Location object
   *
   * @return {Array}           List of available shippers nearby
   */
  findOneAround(geoParams, actParams, opts) {

    let self = this,
      model = new self.modelClass(),
      collectionName = model.collectionName;

    return self.connect().then(db => {

      let collection = db.collection(collectionName),
        queryData = self.findAroundQuery.call(self, geoParams, actParams);

      return collection.aggregate(queryData).limit(1).toArray().then(results => {

        self.log.debug('Selects around driver successfully. Count: ', results.length);
        if (results.length > 0) {
          return BPromise.resolve(results[0].activity);
        }
        return BPromise.resolve(null);

      });

    });
  }
}

module.exports = UserActivityAdapter;
