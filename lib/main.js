#!/usr/bin/env node

/*
 * nodebox
 * https://github.com/sack.io/nodebox
 *
 * Copyright (c) 2015 Ben Sack
 * Licensed under the MIT license.
 */

var Path = require('path')
  , Optionall = require('optionall')
  , FSTK = require('fstk')
  , OS = require('os')
  , Async = require('async')
  , _ = require('underscore')
  , Belt = require('jsbelt')
  , Util = require('util')
  , Winston = require('winston')
  , Events = require('events')
  , Zlib = require('zlib')
  , Crypto = require('crypto')
  , AWS = require('aws-sdk')
  , Spinner = require('its-thinking')
;

module.exports = function(O){
  var Opts = O || new Optionall({
                                  '__dirname': Path.resolve(module.filename + '/../..')
                                , 'file_priority': ['package.json', 'environment.json', 'config.json']
                                });

  var S = new (Events.EventEmitter.bind({}))();
  S.settings = Belt.extend({
    'log_level': 'debug'
  , 'spin_pattern': 4
  , 'stat_concurrency': OS.cpus().length * OS.cpus().length
  , 'sync_concurrency': OS.cpus().length
  , 'upload_concurrency': 5
  }, Opts);

  var log = Opts.log || new Winston.Logger();
  if (!Opts.log) log.add(Winston.transports.Console, {'level': S.settings.log_level, 'colorize': true, 'timestamp': false});
  S.log = log;

  S['spinner'] = new Spinner(S.settings.spin_pattern);

  //////////////////////////////////////////////////////////////////////////////
  ////                           METHODS                                    ////
  //////////////////////////////////////////////////////////////////////////////

  S['start'] = function(options, callback){
    var a = Belt.argulint(arguments)
      , self = this;
    a.o = _.defaults(a.o, {
      'loop': false
    , 'wait_int': 500
    , 'priority': -Infinity
      //start_path
    });

    self.statQueue.push({'path': a.o.start_path}, a.o.priority);

    setTimeout(function(){
      Async.until(function(){ return !S.statQueue.length() && S.syncQueue.length(); }
      , function(next){ return setTimeout(next, 500); }
      , a.cb);
    }, 2000);

    return S;
  };

  S['sync'] = function(options, callback){
    options.part_size = 1000000;
    options.storage_class = 'REDUCED_REDUNDANCY';
    options.acl = 'private';
    options.algorithm = 'md5';
    options.bucket = S.settings.bucket;
    options.file_prefix = S.settings.file_prefix;
    options.checksum_prefix = S.settings.checksum_prefix;
    options.upload_concurrency = S.settings.upload_concurrency;

    var s3 = new AWS.S3(_.omit(S.settings.aws, ['bucket']))
      , gb = {}
      , ocb = _.once(function(){
          gb['ended'] = true;
          return callback.apply(this, arguments);
        });

    return Async.waterfall([
      function(cb){
        gb['fstream'] = FSTK._fs.createReadStream(options.file_path);

        //log.info('Opened [' + options.file_path + ']');

        gb.fstream.on('error', ocb);

        return cb();
      }
    , function(cb){
        return s3.getObject({
          'Bucket': options.bucket
        , 'Key': options.file_prefix + options.file_path.replace(/^\//, '') + '.checksums.gz'
        }, function(err, data){
          if (err) return cb(err.code === 'NoSuchKey' ? undefined : err);

          gb['s3_obj'] = data;

          return cb();
        });
      }
    , function(cb){
        if (!Belt.get(gb, 's3_obj.Body')) return cb();

        //log.info('Received object [' + options.file_prefix + options.file_path + '][' + options.bucket + ']');

        return Zlib.gunzip(gb.s3_obj.Body, Belt.cs(cb, gb, 's3_gunzip', 1, 0));
      }
    , function(cb){
        if (!gb.s3_gunzip) return cb();

        gb['s3_manifest'] = gb.s3_gunzip.toString().split('\n');

        //log.info('Unzipped object [' + options.file_prefix + options.file_path + '][' + options.bucket + ']');

        return cb();
      }
    , function(cb){
        gb['manifest'] = [];
        gb['manifest_object'] = {};

        gb.s3_manifest = gb.s3_manifest || [];

        var index = 0;

        gb.fstream.on('close', function(){
          //log.info('Close [' + options.file_path + ']');
          return gb['fstream_closed'] = true;
        });

        gb.upload_queue = Async.queue(function(task, next){
          var _gb = {};
          return Async.waterfall([
            function(cb){
              return s3.headObject({
                'Bucket': options.bucket
              , 'Key': options.checksum_prefix + task.sum
              }, function(err, obj){
                if (obj){
                  //log.warn('Part on S3 [' + task.sum + '][' + task.index + ']');
                  return next();
                }

                if (!err || err.code !== 'NotFound'){
                  //if (err) log.error(error);
                  return next(err);
                }

                return cb();
              });
            }
          , function(cb){
              return Zlib.gzip(task.part, Belt.cs(cb, _gb, 'gz', 1, 0));
            }
          ], function(err){
            if (err) return next(err);

            //log.info('Uploading part [' + task.sum + '][' + task.index + ']');

            return s3.putObject({
              'Bucket': options.bucket
            , 'Key': options.checksum_prefix + task.sum
            , 'Body': _gb.gzip
            , 'StorageClass': options.storage_class
            , 'ACL': options.acl
            }, Belt.cw(next, 0));
          });
        }, options.upload_concurrency);

        gb.fstream.on('readable', function(){
          var ind, part, sum;
          while (null !== (part = gb.fstream.read(options.part_size)) && !Belt.isNull(ind = index) && ++index){
            sum = Crypto.createHash(options.algorithm).update(part).digest('hex');
            gb.manifest[ind] = sum;

            //log.info('Read part [' + sum + '][' + ind + ']');

            if (sum === gb.s3_manifest[ind]){
              //log.info('Part matched manifest [' + sum + ']');
            } else {
              //log.error('Part did not match manifest [' + sum + ']');
              gb['changed'] = true;
            }

            if (gb.manifest_object[sum]){
              //log.warn('Part already uploaded [' + sum + ']');
              continue;
            }

            gb.manifest_object[sum] = true;

            gb.upload_queue.push({'sum': sum, 'part': part, 'index': ind});
          }
        });

        Async.until(function(){ return !gb.upload_queue.length() && gb.fstream_closed; }
        , function(next){ return setTimeout(next, 500); }
        , cb);
      }
    , function(cb){
        if (!gb.changed) return cb();

        return Zlib.gzip(gb.manifest.join('\n'), Belt.cs(cb, gb, 'gzip_manifest', 1, 0));
      }
    , function(cb){
        if (!gb.gzip_manifest) return cb();

        //log.info('Uploading part manifest');

        return s3.putObject({
          'Bucket': options.bucket
        , 'Key': options.file_prefix + options.file_path.replace(/^\//, '') + '.checksums.gz'
        , 'Body': gb.gzip_manifest
        }, cb);
      }
    ], function(err){
      return ocb(err, gb.changed);
    });
  };

  //////////////////////////////////////////////////////////////////////////////
  ////                           QUEUES                                     ////
  //////////////////////////////////////////////////////////////////////////////

  S['statQueue'] = Async.priorityQueue(function(task, next){
    return FSTK._fs.lstat(task.path, function(err, stat){
      if (err){
        return next();
      }

      if (stat.isSymbolicLink()){
        //TODO - put object to represent link
        return next();
      }

      if (stat.isDirectory()) {
        var mtime = -1 * stat.mtime.getTime();
        return FSTK._fs.readdir(task.path, function(err, paths){
          if (err || !_.any(paths)){
            return next();
          }

          _.each(paths, function(p){
            return S.statQueue.push({
              'path': task.path + '/' + p
            }, mtime);
          });

          return next();
        });
      }

      if (!stat.isFile()){
        //TODO - put object to represent non-file
        return next();
      }

      S.syncQueue.push(task, -1 * stat.mtime.getTime());

      return next();
    });
  }, S.settings.stat_concurrency);

  S['syncQueue'] = Async.priorityQueue(function(task, next){
    return S.sync({
      'file_path': task.path
    }, function(err, changed){
      log.info(arguments);
      log.info('SYNCED [' + task.path + ']');
      return next();
    });
  }, S.settings.sync_concurrency);

  return S;
};

if (require.main === module){
  var M = new module.exports()
    , method = _.find(_.keys(M.settings.argv), function(k){ return M[k]; });

  if (!method){
    M.log.error('Method not found');
    process.exit(1);
  } else {
    M[method](M.settings.argv, function(err, res){
      if (err) M.log.error(err);
      if (res) M.log.info(Belt.stringify(res));
      return process.exit(err ? 1 : 0);
    });
  }
}
