'use strict';
global.Promise = require('bluebird');
var path = require('path');
global.appName = path.basename(__dirname);

var Logger = require('node-bunyan-gcalendar');
var calendar_level = 'fatal';

var bunyanoptions = {
    name: process.env.HEROKU_APP_NAME + ':' + appName,
    streams: [{
        level: 'debug',
        stream: process.stdout
    }, {
        level: 'trace',
        path: path.resolve(process.env.NJSAGENT_APPROOT + '/logs/' + appName + '.log'),
    }]
};

var log;

new Logger(bunyanoptions, process.env.NBGC_AES, process.env.NBGC_KEY, calendar_level).then(function logOk(l) {

    log = l;
    log.info('Logging started');

    if (! process.env.TWCOLLECTOR_PGSQL) {
        log.error('Missing TWCOLLECTOR_PGSQL environment variable');
        process.exit(1);
    } else {
        log.trace('TWCOLLECTOR_PGSQL=' + process.env.TWCOLLECTOR_PGSQL);
    }
    if (! process.env.TWCOLLECTOR_TWITTER_CONSUMER_KEY) {
        log.error('Missing TWCOLLECTOR_TWITTER_CONSUMER_KEY environment variable');
        process.exit(1);
    } else {
        log.trace('TWCOLLECTOR_TWITTER_CONSUMER_KEY=' + process.env.TWCOLLECTOR_TWITTER_CONSUMER_KEY);
    }
    if (! process.env.TWCOLLECTOR_TWITTER_CONSUMER_SECRET) {
        log.error('Missing TWCOLLECTOR_TWITTER_CONSUMER_SECRET environment variable');
        process.exit(1);
    } else {
        log.trace('TWCOLLECTOR_TWITTER_CONSUMER_SECRET=' + process.env.TWCOLLECTOR_TWITTER_CONSUMER_SECRET);
    }
    if (! process.env.TWCOLLECTOR_TWITTER_ACCESS_TOKEN_KEY) {
        log.error('Missing TWCOLLECTOR_TWITTER_ACCESS_TOKEN_KEY environment variable');
        process.exit(1);
    } else {
        log.trace('TWCOLLECTOR_TWITTER_ACCESS_TOKEN_KEY=' + process.env.TWCOLLECTOR_TWITTER_ACCESS_TOKEN_KEY);
    }
    if (! process.env.TWCOLLECTOR_TWITTER_ACCESS_TOKEN_SECRET) {
        log.error('Missing TWCOLLECTOR_TWITTER_ACCESS_TOKEN_SECRET environment variable');
        process.exit(1);
    } else {
        log.trace('TWCOLLECTOR_TWITTER_ACCESS_TOKEN_SECRET=' + process.env.TWCOLLECTOR_TWITTER_ACCESS_TOKEN_SECRET);
    }
    if (! process.env.TWCOLLECTOR_MODE) {
        log.error('Missing TWCOLLECTOR_MODE environment variable');
        process.exit(1);
    } else {
        log.trace('TWCOLLECTOR_MODE=' + process.env.TWCOLLECTOR_MODE);
    }
    if (! process.env.TWCOLLECTOR_SOURCE) {
        log.error('Missing TWCOLLECTOR_SOURCE environment variable');
        process.exit(1);
    } else {
        log.trace('TWCOLLECTOR_SOURCE=' + process.env.TWCOLLECTOR_SOURCE);
    }                        

    //
    // Handle signals
    //
    var stopSignals = [
        'SIGHUP', 'SIGINT', 'SIGQUIT', 'SIGILL', 'SIGTRAP', 'SIGABRT',
        'SIGBUS', 'SIGFPE', 'SIGUSR1', 'SIGSEGV', 'SIGUSR2', 'SIGTERM'
    ];
    stopSignals.forEach(function(signal) {
        process.once(signal, function(s) {
            log.info('Got signal, exiting...');
            setTimeout(function() {
                process.exit(1);
            }, 1000);
        });
    });
    process.once('uncaughtException', function(err) {
        log.fatal('Uncaught Exception. Killing jobs in agents');
        log.error(err);
        setTimeout(function() {
            process.exit(1);
        }, 1000);
    });


setInterval(function() {
    var reply = {
        type: 'process:msg',
        data: {
            time: new Date().toISOString()
        }
    };

    log.info(new Date().toISOString());

}, 3000);    

}, function logNotOK(err){
    console.error('Logging start failed: ', err);
});

