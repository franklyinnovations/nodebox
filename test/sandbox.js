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
  , Spinner = require('its-thinking')
  , Filewalk = require('../lib/filewalk.js')
;

var O = new Optionall({
                       '__dirname': Path.resolve(module.filename + '/../..')
                     , 'file_priority': ['package.json', 'environment.json', 'config.json']
                     });

var Log = new Winston.Logger();
Log.add(Winston.transports.Console, {'level': O.log_level || 'debug', 'colorize': true, 'timestamp': false});

var Spinner = new Spinner(O.spin_pattern || 2);

var GB = {};

var FW = new Filewalk(O);

if (O.VERBOSE) FW.on('readdir', function(path, err, contents){
  if (err){
    Log.error(Belt.stringify({
      'path': path
    , 'error': err
    }));
  } else {
    Log.info(Belt.stringify({
      'path': path
    , 'contents': contents
    }));
  }
  return;
});

if (O.LOG) FW.on('readdir', function(path, err, files, part){
  Log[err ? 'error' : 'info'](path + (Belt.isNull(part) ? '' : ':::' + part));
});

GB.contents = {
  'files': []
, 'dirs': []
};
if (O.STORE){
  FW.on('readdir', function(path, err, contents){
    if (_.any(contents.files)) GB.contents.files = GB.contents.files.concat(contents.files);
    if (_.any(contents.dirs)) GB.contents.dirs = GB.contents.dirs.concat(contents.dirs);
    return;
  });
  FW.on('readdir:part', function(path, files){
    GB.contents.files = GB.contents.files.concat(files);
    return;
  });
}

Spinner.start();

Log.profile('filewalk');
FW.start({'start_path': '/mnt/F/Dropbox'}, function(){
  Spinner.stop();
  Log.profile('filewalk');
  Log.info(GB.contents.files.length);
  Log.info(GB.contents.dirs.length);
  process.exit();
});

