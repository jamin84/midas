var _ = require('underscore');
var async = require('async');
var tools = require('../util/tools.js');
var indicatorMACD = require('../indicators/MACD');

var processor = function(indicatorSettings, storage, logger) {

  this.initialDBWriteDone = false;
  this.storage = storage;
  this.logger = logger;
  this.MACD = new indicatorMACD(indicatorSettings.options, logger);

  _.bindAll(this, 'updateCandleStick', 'createCandleSticks', 'processTickUpdate', 'processUpdate', 'updateCandleDB', 'updateTickDB');

};

//---EventEmitter Setup
var Util = require('util');
var EventEmitter = require('events').EventEmitter;
Util.inherits(processor, EventEmitter);
//---EventEmitter Setup

processor.prototype.updateTickDB = function(ticks) {
  var toBePushed = [];

  ticks.forEach(function(tick) {
    //this.logger.log(tick.date+' | '+tick.price+' | '+tick.amount+' | '+tick.type);
    toBePushed.push({'date':tick.date,'price':tick.price,'amount':tick.amount,'type':tick.type});
  }.bind(this));

  this.storage.pushTicks(toBePushed, this.processTickUpdate);

};

processor.prototype.updateCandleStick = function (candleStickSizeMinutes, candleStick, tick) {

  if(!candleStick[candleStickSizeMinutes].open) {

    candleStick[candleStickSizeMinutes].open = tick.price;
    candleStick[candleStickSizeMinutes].high = tick.price;
    candleStick[candleStickSizeMinutes].low = tick.price;
    candleStick[candleStickSizeMinutes].close = tick.price;
    candleStick[candleStickSizeMinutes].volume = tick.amount;
    candleStick[candleStickSizeMinutes].vwap = tick.price;

  } else {

    var currentVwap = candleStick[candleStickSizeMinutes].vwap * candleStick[candleStickSizeMinutes].volume;
    var newVwap = tick.price * tick.amount;

    candleStick[candleStickSizeMinutes].high = _.max([candleStick[candleStickSizeMinutes].high, tick.price]);
    candleStick[candleStickSizeMinutes].low = _.min([candleStick[candleStickSizeMinutes].low, tick.price]);

    candleStick[candleStickSizeMinutes].volume = tools.round(candleStick[candleStickSizeMinutes].volume + tick.amount, 8);
    candleStick[candleStickSizeMinutes].vwap = tools.round((currentVwap + newVwap) / candleStick[candleStickSizeMinutes].volume, 8);

  }

  candleStick[candleStickSizeMinutes].close = tick.price;

  return candleStick;

};

processor.prototype.createCandleSticks = function(ticks, callback) {

  if(ticks.length > 0) {

    this.storage.getLastNonEmptyPeriod('1', function(err, lastStoragePeriod) {

      this.storage.getLastNonEmptyClose('1', function(err, lastNonEmptyClose) {

        var candleStickSizeSeconds = 60;

        var toBePushed = [];

        var indicator = {};

        var previousClose = lastNonEmptyClose;

        var tickTimeStamp = ticks[0].date;

        var firstTickCandleStick = (Math.floor(ticks[0].date/candleStickSizeSeconds)*candleStickSizeSeconds);

        if(lastStoragePeriod < firstTickCandleStick && lastStoragePeriod !== 0) {
          tickTimeStamp = lastStoragePeriod + candleStickSizeSeconds;
        }

        var now = tools.unixTimeStamp(new Date().getTime());

        var startTimeStamp = (Math.floor(tickTimeStamp/candleStickSizeSeconds)*candleStickSizeSeconds);
        var stopTimeStamp = (Math.floor(now/candleStickSizeSeconds)*candleStickSizeSeconds);

        var endTimeStamp = startTimeStamp + candleStickSizeSeconds;

        while(endTimeStamp < ticks[0].date) {

          toBePushed.push({'period':startTimeStamp, '1min': {'open':previousClose,'high':previousClose,'low':previousClose,'close':previousClose,'volume':0, 'vwap':previousClose, 'macd': undefined, 'macdSignal': undefined, 'macdHistogram': undefined}});

          startTimeStamp = endTimeStamp;
          endTimeStamp = endTimeStamp + candleStickSizeSeconds;

        }

        var currentCandleStick = {'period':startTimeStamp, '1min': {'open':undefined,'high':undefined,'low':undefined,'close':undefined,'volume':0,'vwap':undefined, 'macd': undefined, 'macdSignal': undefined, 'macdHistogram': undefined}};

        ticks.forEach(function(tick) {

          tickTimeStamp = tick.date;

          indicator = this.MACD.calculateFromTick(tick);

          if(toBePushed.length > 0) {
            previousClose = _.last(toBePushed)['1min'].close;
          }

          while(tickTimeStamp >= endTimeStamp + candleStickSizeSeconds) {

            if(currentCandleStick['1min'].volume > 0) {
              toBePushed.push(currentCandleStick);
            }

            startTimeStamp = endTimeStamp;
            endTimeStamp = endTimeStamp + candleStickSizeSeconds;

            toBePushed.push({'period':startTimeStamp, '1min': {'open':previousClose,'high':previousClose,'low':previousClose,'close':previousClose,'volume':0, 'vwap':previousClose, 'macd': indicator.macd, 'macdSignal' : indicator.macdSignal, 'macdHistogram' : indicator.macdHistogram}});

          }

          if(tickTimeStamp >= endTimeStamp) {

            if(currentCandleStick['1min'].volume > 0) {
              toBePushed.push(currentCandleStick);
            }

            startTimeStamp = endTimeStamp;
            endTimeStamp = endTimeStamp + candleStickSizeSeconds;

            currentCandleStick = {'period':startTimeStamp, '1min': {'open':undefined,'high':undefined,'low':undefined,'close':undefined,'volume':0, 'vwap':undefined, 'macd': indicator.macd, 'macdSignal' : indicator.macdSignal, 'macdHistogram' : indicator.macdHistogram}};

          }

          if(tickTimeStamp >= startTimeStamp && tickTimeStamp < endTimeStamp) {

            currentCandleStick = this.updateCandleStick('1min', currentCandleStick, tick);

          }

        }.bind(this));

        if(currentCandleStick['1min'].volume > 0) {

          toBePushed.push(currentCandleStick);

          startTimeStamp = endTimeStamp;
          endTimeStamp = endTimeStamp + candleStickSizeSeconds;

        }

        if(toBePushed.length > 0) {
          previousClose = _.last(toBePushed)['1min'].close;
        }


        for(var i = startTimeStamp;i <= stopTimeStamp;i = i + candleStickSizeSeconds) {

          var beginPeriod = i;
          var endPeriod = beginPeriod + candleStickSizeSeconds;

          //toBePushed.push({'period':beginPeriod,'open':previousClose,'high':previousClose,'low':previousClose,'close':previousClose,'volume':0, 'vwap':previousClose, 'MACD': indicator});
          toBePushed.push({'period':beginPeriod, '1min' : {'open':previousClose,'high':previousClose,'low':previousClose,'close':previousClose,'volume':0, 'vwap':previousClose, 'macd': indicator.macd, 'macdSignal' : indicator.macdSignal, 'macdHistogram' : indicator.macdHistogram}});

        }

        this.storage.pushBulk('1', toBePushed, callback);

      }.bind(this));

    }.bind(this));

  } else {

    callback(null);

  }

};

processor.prototype.processUpdate = function(err) {

  if(err) {

    var parsedError = JSON.stringify(err);

    if(err.stack) {
      parsedError = err.stack;
    }

    this.logger.error('Couldn\'t create candlesticks due to a database error');
    this.logger.error(parsedError);

    process.exit();

  } else {

    this.storage.getLastNCandles('1', 1, function(err, candleSticks) {

      var latestCandleStick = candleSticks[0];

      if(!this.initialDBWriteDone) {

        this.emit('initialDBWrite');
        this.initialDBWriteDone = true;

      } else {

        this.emit('update', latestCandleStick);

      }

    }.bind(this));

  }

};

processor.prototype.processTickUpdate = function(err) {

  if(err) {

    var parsedError = JSON.stringify(err);

    if(err.stack) {
      parsedError = err.stack;
    }

    this.logger.error('Couldn\'t create ticks due to a database error');
    this.logger.error(parsedError);

    process.exit();

  } else {

    this.storage.getLastTick(1, function(err, ticks) {

      var latestTick = ticks[0];

      if(!this.initialDBWriteDone) {

        this.emit('initialDBWrite');
        this.initialDBWriteDone = true;

      } else {

        this.emit('update', latestTick);

      }

    }.bind(this));

  }

};

processor.prototype.updateCandleDB = function(candleStickSizeMinutes, ticks) {

  this.storage.getLastNonEmptyPeriod(candleStickSizeMinutes, function(err, period) {

    var newTicks = _.filter(ticks,function(tick){

      return tick.date >= period;

    });

    this.createCandleSticks(newTicks, this.processUpdate);

  }.bind(this));

};

module.exports = processor;