#!/usr/bin/env node

var Path = require('path')
  , Optionall = require('optionall')
  , FSTK = require('fstk')
  , Async = require('async')
  , _ = require('underscore')
  , Belt = require('jsbelt')
  , Util = require('util')
  , Crypto = require('crypto')
  , Winston = require('winston')
  , Events = require('events')
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
  }, Opts);

  var log = Opts.log || new Winston.Logger();
  if (!Opts.log) log.add(Winston.transports.Console, {'level': S.settings.log_level, 'colorize': true, 'timestamp': false});
  S['log'] = log;

  S['upload'] = function(options, callback){
    var self = this;
    /*
      bucket
      checksum_prefix
      file_prefix
      part_size
      file_path
    */

    options.part_size = 1000000;
    options.algorithm = 'md5';

    var fStream = FSTK._fs.createReadStream(options.file_path)
      , cSums = [], oSums = {};
    fStream.on('readable', function(){
      var part, sum;
      while (null !== (part = fStream.read(options.part_size))){
        sum = Crypto.createHash(options.algorithm).update(part).digest('hex');
        log.info(sum);
        cSums.push(sum);
        oSums[sum] = true;
      }
    });

    fStream.on('close', function(){
      console.log(cSums.length);
      console.log(_.size(oSums));
    });

    /*
      get file's s3 manifest of checksums
      read in part_size of file
      compute checksum, store read part in ram
      if checksum matches manifest, go to next iteration

      if checksum does not match
        check for checksum on s3
          if not present, upload part
        modify file manifest

      when file has been read in its entirety, if manifest changed (or did not exist)
        upload new manifest
    */
  };

  S['downloadPath'] = function(options, callback){
    /*
      bucket
      checksum_prefix
      file_prefix
    */

    /*
      get file's s3 manifest of checksums
      open write stream
      download checksums from manifest in order to write stream
    */
  };

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
