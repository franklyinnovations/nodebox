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
;

module.exports = function(O){
  var Opts = O || new Optionall({
                                  '__dirname': Path.resolve(module.filename + '/../..')
                                , 'file_priority': ['package.json', 'environment.json', 'config.json']
                                });

  var S = new (Events.EventEmitter.bind({}))();
  S.settings = Belt.extend({
    'log_level': 'info'
  , 'path_suffix': '/./'
  , 'cache_ttl': 600000
  , 'poll_wait': 2000
  , 'pruner_int': 60000
  , 'dir_list_concurrency': 25
  , 'upload_concurrency': 25
  }, Opts);

  var log = Opts.log || new Winston.Logger();
  if (!Opts.log) log.add(Winston.transports.Console, {'level': S.settings.log_level, 'colorize': true, 'timestamp': false});
  S.log = log;

  S['s3'] = new AWSTK.s3(S.settings.aws);
  S['s3_api'] = S.s3._API;

  S['dirCache'] = {};
  S['dirQueue'] = Async.priorityQueue(function(options, next){
    if (S.dirCache[options.path].updated_at){
      while (S.dirCache.listeners.length) (S.dirCache[options.path].listeners.shift())(null, S.dirCache[options.path].contents);
      return next();
    }

    var path = options.path + S.settings.path_suffix;

    return Belt.clp({
      'int': S.settings.poll_wait
    , 'this': S.s3
    , 'meth': S.s3.listAllObjects
    , 'args': [{
        'Prefix': path
      , 'Bucket': S.settings.aws.bucket
      }]
    }, function(err, contents){
      S.dirCache[options.path].contents = _.any(contents) ? _.object(_.pluck(contents, 'Key'), _.pluck(contents, 'ETag')) : {};
      S.dirCache[options.path].updated_at = new Date().getTime();

      while (S.dirCache[options.path].listeners.length) (S.dirCache[options.path].listeners.shift())(null, S.dirCache[options.path].contents);

      return next();
    });
  }, S.settings.dir_list_concurrency);
  S['dirCachePruner'] = setInterval(function(){
    var ttl = new Date().getTime() - S.settings.cache_ttl;

    _.keys(S.dirCache).forEach(function(k){
      var p = S.dirCache[k];
      if (p.updated_at && p.updated_at < ttl) delete S.dirCache[k];
      return;
    });
  }, S.settings.pruner_int);


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
      function(cb){
        return S.getDirectoryContents(rp, cb, options.mtime);
      }
    , function(contents, cb){
        var key = rp + S.settings.path_suffix + fn;

        if (contents[key] && (options.type !== 'file' || contents[key] === options.checksum)) return cb(true);

        options.key = key;
        options.callback = cb;
        return S.syncFileQueue.push(options);
      }
    ], callback);
  };

  S['syncFileQueue'] = Async.priorityQueue(function(options, next){
    return Belt.clp({
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
    });
  }, S.settings.upload_concurrency);

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
