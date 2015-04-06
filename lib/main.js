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
  , Async = require('async')
  , _ = require('underscore')
  , Belt = require('jsbelt')
  , Util = require('util')
  , Winston = require('winston')
  , Events = require('events')
  , Filewalk = require('./filewalk.js')
  , S3 = require('./s3.js')
  , Spinner = require('its-thinking')
;

module.exports = function(O){
  var Opts = O || new Optionall({
                                  '__dirname': Path.resolve(module.filename + '/../..')
                                , 'file_priority': ['package.json', 'environment.json', 'config.json']
                                });

  var S = new (Events.EventEmitter.bind({}))();
  S.settings = Belt.extend({
    'log_level': 'info'
  , 'spin_pattern': 2
  }, Opts);

  var log = Opts.log || new Winston.Logger();
  if (!Opts.log) log.add(Winston.transports.Console, {'level': S.settings.log_level, 'colorize': true, 'timestamp': false});
  S.log = log;

  S['spinner'] = new Spinner(S.settings.spin_pattern);

  S['backup'] = function(options, callback){
    var a = Belt.argulint(arguments)
      , self = this;
    a.o = _.defaults(a.o, {
      //start_path
    /*  'readdir_concurrency': 6
    , 'stat_concurrency': 6
    , 'checksum_concurrency': 6*/
      'upload_concurrency': 5
    });

    self.spinner.start();

    var s3 = new S3(Belt.extend({}, S.settings, a.o));
    var fw = new Filewalk(Belt.extend({}, S.settings, a.o));

    /*if (a.o.verbose) fw.on('readdir', function(path, err, files, part){
      self.log[err ? 'error' : 'info'](path + (Belt.isNull(part) ? '' : ':::' + part));
    });*/

    if (a.o.verbose){
      fw.on('readdir', function(path, err, files, part){
        _.each(files, function(f){
          s3.syncFile(f, function(nosync, sync){
            return self.log[sync ? 'info' : 'error']('S3 SYNC: ' + f.path);
          });
          return
        });
        return;
      });
    } else {
      fw.on('readdir', function(path, err, files, part){
        _.each(files, function(f){
          s3.syncFile(f, Belt.np);
          return
        });
        return;
      });
    }

    self.spinner.start();
    self.log.profile(a.o.start_path);

    fw.start(a.o, function(err){
      self.spinner.stop();
      self.log.profile(a.o.start_path);
      return a.cb(err);
    });

    return self;
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
