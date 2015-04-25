#!/usr/bin/env node

var Path = require('path')
  , Optionall = require('optionall')
  , FSTK = require('fstk')
  , Async = require('async')
  , _ = require('underscore')
  , Belt = require('jsbelt')
  , Util = require('util')
  , Winston = require('winston')
  , OS = require('os')
  , Events = require('events')
;

module.exports = function(O){
  var Opts = O || new Optionall({
                                  '__dirname': Path.resolve(module.filename + '/../..')
                                , 'file_priority': ['package.json', 'environment.json', 'config.json']
                                });

  var S = new (Events.EventEmitter.bind({}))();
  S.settings = Belt.extend({
    'log_level': 'info'
  , 'readdir_concurrency': OS.cpus().length
  , 'stat_concurrency': OS.cpus().length
  , 'checksum_concurrency': OS.cpus().length * OS.cpus().length
  , 'part_length': OS.cpus().length
  , 'part_size': Math.floor(OS.cpus().length / 2) || 1
  , 'empty_file_size': -1
  }, Opts);

  var log = Opts.log || new Winston.Logger();
  if (!Opts.log) log.add(Winston.transports.Console, {'level': S.settings.log_level, 'colorize': true, 'timestamp': false});
  S.log = log;

  S['readdirQueue'] = Async.priorityQueue(function(options, next){
    return FSTK._fs.readdir(options.path, function(err, paths){
      if (err){
        S.emit('readdir', options.path, err);
        return next();
      }

      if (paths.length > S.settings.part_size){
        var contents = {
          'files': []
        , 'dirs': []
        , 'part': 0
        , 'part_size': 0
        };
        return Async.eachLimit(paths, S.settings.part_size, function(p, _cb){
          return S.statQueue.unshift({
            'path': options.path + '/' + p
          , 'cb': function(file, dir){
              if (file){
                contents.files.push(file);
                contents.part_size += file.size;
                if (contents.files.length >= S.settings.part_length
                   || contents.part_size >= S.settings.part_size){
                  S.emit('readdir', options.path, null, _.clone(contents.files), contents.part, contents);
                  contents.files = [];
                  contents.part++;
                  contents.part_size = 0;
                }
              } else {
                contents.dirs.push(dir);
              }
              return _cb();
            }
          });
        }, function(err){
          if (_.any(contents.files)) S.emit('readdir', options.path, err, contents.files, contents.part, contents);
          return next();
        });

      } else {
        var contents = {
          'files': []
        , 'dirs': []
        };
        return Async.each(paths, function(p, _cb){
          return S.statQueue.unshift({
            'path': options.path + '/' + p
          , 'cb': function(file, dir){
              file ? contents.files.push(file) : contents.dirs.push(dir);
              return _cb();
            }
          });
        }, function(err){
          S.emit('readdir', options.path, err, contents.files, null);
          return next();
        });
      }
    });
  }, S.settings.readdir_concurrency);

  S['statQueue'] = Async.queue(function(options, next){
    var sint = setInterval(function(){ return console.log('STAT: ' + options.path); }, 5000);
    return FSTK._fs.lstat(options.path, function(err, stat){
      if (err){
        options.cb({'path': options.path, 'error': err.code});
        clearInterval(sint);
        return next();
      }

      if (stat.isSymbolicLink()){
        options.cb({'path': options.path, 'type': 'symlink', 'ctime': stat.ctime.getTime(), 'mtime': -1 * stat.mtime.getTime()});
        clearInterval(sint);
        return next();
      } else if (stat.isDirectory()) {
        var mtime = -1 * stat.mtime.getTime();
        options.cb(null, {'path': options.path, 'type': 'directory', 'ctime': stat.ctime.getTime(), 'mtime': mtime});
        S.readdirQueue.push({'path': options.path}, mtime);
        clearInterval(sint);
        return next();
      } else {
        var type = stat.isFile() ? 'file'
                 : stat.isSocket() ? 'socket'
                 : stat.isFIFO() ? 'fifo'
                 : stat.isBlockDevice() ? 'blockdevice'
                 : stat.isCharacterDevice() ? 'chardevice'
                 : 'other';

        if (type === 'file' && stat.size > S.settings.empty_file_size){
          var mtime = -1 * stat.mtime.getTime();
          return S.checksumQueue.push({'path': options.path, 'cb': function(err, cs){
            options.cb({'path': options.path, 'type': type, 'checksum': cs, 'error': err ? err.code : undefined
            , 'ctime': stat.ctime.getTime(), 'mtime': mtime, 'size': stat.size});
            clearInterval(sint);
            return next();
          }}, mtime);
        }

        options.cb({'path': options.path, 'type': type, 'ctime': stat.ctime.getTime(), 'mtime': stat.mtime.getTime(), 'size': stat.size});
      }

      clearInterval(sint);
      return next();
    });
  }, S.settings.stat_concurrency);

  S['checksumQueue'] = Async.priorityQueue(function(options, next){
    return FSTK.fileChecksum(options.path, function(err, checksum){
      options.cb(err, checksum);
      return next();
    });
  }, S.settings.checksum_concurrency);

  S['start'] = function(options, callback){
    var a = Belt.argulint(arguments)
      , self = this;
    a.o = _.defaults(a.o, {
      'loop': false
    , 'wait_int': 1000
    , 'priority': -Infinity
      //start_path
    });

    var ocb = _.once(a.cb)
      , stop = _.bind(function(){
         if (this.readdirQueue.idle()){
           return ocb();
         }
       }, self);

    self.readdirQueue.drain = function(){
      return setTimeout(stop, a.o.wait_int);
    };

    self.readdirQueue.push({'path': a.o.start_path}, a.o.priority);
    return S;
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
