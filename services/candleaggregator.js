var _ = require('underscore');
var tools = require('../util/tools.js');

var aggregator = function(candleStickSizeMinutesArray, storage, logger) {

  this.storage = storage;
  this.candleStickSizeMinutesArray = candleStickSizeMinutesArray; //now an array
  this.logger = logger;
  this.previousCompleteCandleStickPeriod = [];

  for(var i = 0; i<this.candleStickSizeMinutesArray.length; i++){
    this.previousCompleteCandleStickPeriod[ this.candleStickSizeMinutesArray[i] ] = 0;
  }

  _.bindAll(this, 'update', 'setCandleStickSize');

};

//---EventEmitter Setup
var Util = require('util');
var EventEmitter = require('events').EventEmitter;
Util.inherits(aggregator, EventEmitter);
//---EventEmitter Setup

aggregator.prototype.update = function() {
  this.logger.log('Aggregator updating...');

  //for each candle size in the array
  for(var i = 0; i<this.candleStickSizeMinutesArray.length; i++){
    this.logger.log('* Checking candle size '+this.candleStickSizeMinutesArray[i]+'...');

    this.storage.getLastCompleteAggregatedCandleStick(this.candleStickSizeMinutesArray[i], function(err, candleStickSizeMinutes, completeCandleStick) {

      this.logger.log('** Complete Candelstick: '+JSON.stringify(completeCandleStick));
      if(completeCandleStick) {

        if(this.previousCompleteCandleStickPeriod[candleStickSizeMinutes] === 0) {

          this.previousCompleteCandleStickPeriod[candleStickSizeMinutes] = completeCandleStick.period;

        }

        this.logger.log('***** Complete Candelstick Period: '+completeCandleStick.period+' | previousCompleteCandleStickPeriod: '+this.previousCompleteCandleStickPeriod[ candleStickSizeMinutes ]) 
        if(completeCandleStick.period !== this.previousCompleteCandleStickPeriod[ candleStickSizeMinutes ]) {

          this.logger.log('*** Created a new '+ candleStickSizeMinutes +' candlestick!');
          this.logger.log('JSON: '+JSON.stringify(completeCandleStick));
          this.storage.push(candleStickSizeMinutes, completeCandleStick, function(err){
            if(err) {

              var parsedError = JSON.stringify(err);

              if(err.stack) {
                parsedError = err.stack;
              }

              this.logger.error('Couldn\'t create candlesticks due to a database error');
              this.logger.error(parsedError);

              process.exit();

            } else {

              this.storage.getLastNCandles(candleStickSizeMinutes, 1, function(err, candleSticks) {

                var latestCandleStick = candleSticks[0];
                //this.emit('update', latestCandleStick);

              }.bind(this));

            }

          }.bind(this));

          this.previousCompleteCandleStickPeriod[candleStickSizeMinutes] = completeCandleStick.period;

          this.storage.removeOldDBCandles([candleStickSizeMinutes], function(err) {

            this.emit('update', completeCandleStick);

          }.bind(this));

        }

      }

    }.bind(this));

  }


};

aggregator.prototype.setCandleStickSize = function(candleStickSizeMinutes) {

  this.candleStickSizeMinutes = candleStickSizeMinutes;

  this.storage.getLastCompleteAggregatedCandleStick(this.candleStickSizeMinutes, function(err, completeCandleStick) {

    if(completeCandleStick) {

      this.previousCompleteCandleStickPeriod = completeCandleStick.period;

    }

  }.bind(this));

};

module.exports = aggregator;
