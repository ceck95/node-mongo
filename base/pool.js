/*
 * @Author: toan.nguyen
 * @Date:   2016-04-22 23:16:29
 * @Last Modified by:   toan.nguyen
 * @Last Modified time: 2016-09-06 17:32:46
 */

'use strict';

const Hoek = require('hoek');
const config = require('config');
const BPromise = require('bluebird');
const MongoLogger = require('mongodb').Logger;
const MongoError = require('mongodb').MongoError;
const MongoClient = require('mongodb').MongoClient;

const isDebug = config.has('isDebug') ? config.get('isDebug') : (!process.env.NODE_ENV || process.env.NODE_ENV === 'development');

class MongoDB {

  /**
   * Constructor Mongo DB by client
   *
   * @param {String} name Mongo DB config string
   */
  constructor(name) {
    this.dbs = [];
    this.connect(name);
  }

  /**
   * Connects to mongodb, then run query
   *
   * @param  {String} name Connection config
   *
   * @return {Promise}           Promise result
   */
  connect(name) {
    let self = this;
    name = name || 'default';

    return new BPromise((resolve, reject) => {

      if (self.dbs[name]) {
        return resolve(self.dbs[name]);
      }

      let mongoCfg = config.get('db.mongodb.' + name),
        url = mongoCfg.connection;

      // Use connect method to connect to the Server
      console.log('Connects to mongodb server', mongoCfg);

      let opts = !mongoCfg.options ? {} : Hoek.clone(mongoCfg.options);
      opts.promiseLibrary = BPromise;

      let defaultServerCfg = {
        socketOptions: {
          connectTimeoutMS: 30000,
          socketTimeoutMS: 30000,
        }
      };

      if (opts.server) {
        opts.server = defaultServerCfg;
      } else {
        opts.server = Hoek.applyToDefaults(defaultServerCfg, opts.server);
      }

      MongoClient.connect(url, opts).then((db) => {
        console.log('Connected correctly to server');

        if (mongoCfg.log ? mongoCfg.log.level : false) {
          // Set debug level
          MongoLogger.setLevel(mongoCfg.log.level);
        }

        self.dbs[name] = db;

        return resolve(db);
      }).catch(MongoError, e => {
        console.error(e);
        return reject(e);
      }).catch(e => {
        if (isDebug) {
          throw e;
        }
        return reject(e);
      });
    });
  }
}

let adapter = new MongoDB();

module.exports = adapter;

module.exports.MongoDB = MongoDB;
