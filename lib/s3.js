#!/usr/bin/env node

var Path = require('path')
  , Optionall = require('optionall')
  , FSTK = require('fstk')
  , Async = require('async')
  , _ = require('underscore')
  , Belt = require('jsbelt')
  , Util = require('util')
  , Winston = require('winston')
  , Events = require('events')
  , AWSTK = require('awstk')
  , AWS = require('aws-sdk')
;

module.exports = function(O){
  var Opts = O || new Optionall({
                                  '__dirname': Path.resolve(module.filename + '/../..')
                                , 'file_priority': ['package.json', 'environment.json', 'config.json']
                                });

  var S = new (Events.EventEmitter.bind({}))();
  S.settings = Belt.extend({
    'log_level': 'info'
  , 'path_suffix': '/' //'/./'
  , 'cache_ttl': 600000
  , 'poll_wait': 2000
  , 'pruner_int': 60000
  , 'dir_list_concurrency': 25
  , 'upload_concurrency': 25
  , 'upload_timeout': 15000
  }, Opts);

  var log = Opts.log || new Winston.Logger();
  if (!Opts.log) log.add(Winston.transports.Console, {'level': S.settings.log_level, 'colorize': true, 'timestamp': false});
  S.log = log;

  //S['s3'] = new AWSTK.s3(S.settings.aws);
  //S['s3_api'] = S.s3._API;
  S['aws_params'] = _.omit(S.settings.aws, ['bucket']);
  AWS.config = S.aws_params;
  S['s3_params'] = {'Bucket': S.settings.aws.bucket};

  S['dirCache'] = {};
  S['dirQueue'] = Async.priorityQueue(function(options, next){
    if (S.dirCache[options.path].updated_at){
      while (S.dirCache.listeners.length) (S.dirCache[options.path].listeners.shift())(null, S.dirCache[options.path].contents);
      return next();
    }

    var path = options.path + S.settings.path_suffix;

    /*return Belt.clp({
      'int': S.settings.poll_wait
    , 'this': S.s3
    , 'meth': S.s3.listAllObjects
    , 'args': [{
        'Prefix': path
      , 'Bucket': S.settings.aws.bucket
      }]
    }, function(err, contents){*/
    return S.s3.listAllObjects({
      'Prefix': path
    , 'Bucket': S.settings.aws.bucket
    }, function(err, contents){
      if (err){
        console.log(err);
        console.log(options);
      }

      S.dirCache[options.path].contents = _.any(contents) ? _.object(_.pluck(contents, 'Key'), _.pluck(contents, 'ETag')) : {};
      S.dirCache[options.path].updated_at = new Date().getTime();

      while (S.dirCache[options.path].listeners.length) (S.dirCache[options.path].listeners.shift())(null, S.dirCache[options.path].contents);

      return next();
    });
  }, S.settings.dir_list_concurrency);
  /*S['dirCachePruner'] = setInterval(function(){
    var ttl = new Date().getTime() - S.settings.cache_ttl;

    _.keys(S.dirCache).forEach(function(k){
      var p = S.dirCache[k];
      if (p.updated_at && p.updated_at < ttl) delete S.dirCache[k];
      return;
    });
  }, S.settings.pruner_int);*/

  S['getDirectoryContents'] = function(prefix, callback, priority){
    if (!S.dirCache[prefix]){
      S.dirCache[prefix] = {
        'listeners': [callback]
      };
      return S.dirQueue.push({'path': prefix}, priority);

    } else if (!S.dirCache[prefix].updated_at) {
      return S.dirCache[prefix].listeners.push(callback);

    } else {
      return callback(null, S.dirCache[prefix].contents);

    }
  };

  S['syncFile'] = function(options, callback){
    var rp = options.path.split('/')
      , fn = rp.pop();
    rp = rp.join('/').replace(/^\/+/, '');

    return Async.waterfall([
    /*  function(cb){
        return S.getDirectoryContents(rp, cb, options.mtime);
      }
    ,*/ function(/*contents,*/ cb){
        var key = rp + S.settings.path_suffix + fn;

        //if (contents[key] && (options.type !== 'file' || contents[key] === options.checksum)) return cb(true);

        options.mtime = options.mtime || Infinity;
        options.type = options.type || 'error';
        options.key = key;
        options.callback = cb;
        return S.syncFileQueue.push(options);
      }
    ], callback);
  };

  S['syncFileQueue'] = Async.priorityQueue(function(options, next){
    /*return Belt.clp({
      'int': S.settings.poll_wait
    , 'this': S.s3_api
    , 'meth': S.s3_api.putObject
    , 'args': [{
        'Bucket': S.settings.aws.bucket
      , 'Key': options.key
      , 'Metadata': {
          'mtime': options.mtime.toString()
        , 'type': options.type
        , 'checksum': options.checksum
        }
      , 'Body': options.type === 'file' ? FSTK._fs.createReadStream(options.path) : options.type
      , 'StorageClass': 'REDUCED_REDUNDANCY'
      , 'ACL': 'private'
      }]
    }, function(){
      options.callback(null, true);
      return next();
    });*/

    /*return S.s3_api.putObject({
      'Bucket': S.settings.aws.bucket
    , 'Key': options.key
    , 'Metadata': {
        'mtime': options.mtime.toString()
      , 'type': options.type
      , 'checksum': options.checksum
      }
    , 'Body': options.type === 'file' ? FSTK._fs.createReadStream(options.path) : options.type
    , 'StorageClass': 'REDUCED_REDUNDANCY'
    , 'ACL': 'private'
    }, function(err){
      if (err){
        console.log(err);
        console.log(options);
      }
      options.callback(null, true);
      return next();
    });*/

    /*return S.s3_api.upload({
      'Bucket': S.settings.aws.bucket
    , 'Key': options.key
    , 'Metadata': {
        'mtime': options.mtime.toString()
      , 'type': options.type
      , 'checksum': options.checksum
      }
    , 'Body': options.type === 'file' ? FSTK._fs.createReadStream(options.path) : options.type
    , 'StorageClass': 'REDUCED_REDUNDANCY'
    , 'ACL': 'private'
    }, {
      'partSize': 10 * 1024 * 1024
    , 'queueSize': 5
    }, function(err){
      if (err){
        console.log(err);
        console.log(options);
      }
      options.callback(null, true);
      return next();
    });*/

console.log('start ' + options.path);

  /*S['s3'] = new AWSTK.s3(S.settings.aws);
  S['s3_api'] = S.s3._API;*/

var s3_obj = new AWS.S3(S.aws_params);

    return Async.waterfall([
      function(cb){
  /*S['s3'] = new AWSTK.s3(S.settings.aws);
  S['s3_api'] = S.s3._API;*/

var ocb = _.once(cb);

var timer = setTimeout(function(){
  return ocb(new Error('timeout'));
}, S.settings.upload_timeout);

        return s3_obj.headObject({
          'Bucket': S.settings.aws.bucket
        , 'Key': options.key
        }, function(err, res){
          clearTimeout(timer);
if (res){
  options.etag = res.Metadata.checksum; //res.ETag.replace(/"/g, '');
  console.log('CHECK: [ETAG: ' + options.etag + '][CS: ' + options.checksum + '] ' + options.key);
}

          if (err || (res.Metadata.type !== options.type) || (options.type === 'file' && options.etag !== options.checksum))
            return cb();

          return cb(true);
        });
      }
    , function(cb){
        var ocb = _.once(cb);

var timer = setTimeout(function(){
  return ocb(new Error('timeout'));
}, S.settings.upload_timeout);

console.log('DIFF: [ETAG: ' + options.etag + '][CS: ' + options.checksum + '] ' + options.key);

        if (options.type === 'file'){
          var str = FSTK._fs.createReadStream(options.path);
          str.on('error', function(err){
            console.log(err);
            str.close();
            return ocb(err);
          });
          //str.on('end', Belt.cw(ocb));
        }

        /*return S.s3_api.upload({
          'Bucket': S.settings.aws.bucket
        , 'Key': options.key
        , 'Metadata': {
            'mtime': options.mtime.toString()
          , 'type': options.type
          , 'checksum': options.checksum
          }
        , 'Body': options.type === 'file' ? str : options.type
        //, 'StorageClass': 'REDUCED_REDUNDANCY'
        , 'ACL': 'private'*/
var s3_obj = new AWS.S3(S.aws_params);

        return s3_obj.upload({
          'Bucket': S.settings.aws.bucket
        , 'Key': options.key
        , 'Body': options.type === 'file' && options.size ? str : options.type
        , 'Metadata': {
            'mtime': options.mtime.toString()
          , 'type': options.type
          , 'checksum': options.checksum
          }
        , 'ACL': 'private'
        , 'StorageClass': 'REDUCED_REDUNDANCY'
        }, {
          'queueSize': S.settings.upload_concurrency
        }).on('error', function(err){
          console.log(err);
          return ocb(err);
        }).on('httpUploadProgress', function(stats){
          clearTimeout(timer);
          timer = setTimeout(function(){
            return ocb(new Error('timeout'));
          }, S.settings.upload_timeout);
          return tpart(options.key, stats);
        }).send(function(){ clearTimeout(timer); return ocb.apply(this, arguments); });
      }
    ], function(err){
      if (err && !_.isBoolean(err)){
        console.log(err);
        console.log(options);
      }
      options.callback(err, !err);
      return next();
    });

  }, S.settings.upload_concurrency);

var tpart = _.throttle(function(key, stats){ console.log('...part...[' + key + '][' + ((stats.loaded / stats.total) * 100).toFixed(1) + '%]'); }, 5000);

setInterval(function(){ return S.log.error('QUEUE: ' + S.syncFileQueue.length()); }, 10000);

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
