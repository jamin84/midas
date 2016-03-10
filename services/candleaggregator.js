var _ = require('underscore');
var tools = require('../util/tools.js');
var indicatorMACD = require('../indicators/MACD');

var aggregator = function(indicatorSettings, storage, logger) {

  this.storage = storage;
  this.candleStickSizeMinutesArray = indicatorSettings.candleStickSizeMinutesArray; //now an array
  this.logger = logger;
  this.previousCompleteCandleStick = [];//an array of objects
  this.MACD = new indicatorMACD(indicatorSettings.options, logger);

/*
  for(var i = 0; i<this.candleStickSizeMinutesArray.length; i++){
    this.previousCompleteCandleStickPeriod[ this.candleStickSizeMinutesArray[i] ]['period'] = 0;
  }
*/
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

        if(!this.previousCompleteCandleStick[candleStickSizeMinutes]) {

          this.previousCompleteCandleStick[candleStickSizeMinutes] = completeCandleStick; //save whole thing to do MACD comparison w/o another lookup

        }

        this.logger.log('***** Complete Candelstick Period: '+completeCandleStick.period+' | previousCompleteCandleStickPeriod: '+this.previousCompleteCandleStick[ candleStickSizeMinutes ].period) 
        if(completeCandleStick.period !== this.previousCompleteCandleStick[ candleStickSizeMinutes ].period) {

          this.logger.log('\n*** Created a new '+ candleStickSizeMinutes +'min candlestick!');
          this.logger.log('JSON: '+JSON.stringify(completeCandleStick));

          var indicator = this.MACD.calculateFromCandles(completeCandleStick, this.previousCompleteCandleStick[ candleStickSizeMinutes ]);

          this.logger.error('\n* Created new '+candleStickSizeMinutes+'min MACD indicator...');
          this.logger.log('JSON: '+JSON.stringify(indicator));

          completeCandleStick['MACD'] = indicator;

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
                this.emit('update', candleStickSizeMinutes, latestCandleStick);

              }.bind(this));

            }

          }.bind(this));

          this.previousCompleteCandleStick[candleStickSizeMinutes] = completeCandleStick;

          this.storage.removeOldDBCandles([candleStickSizeMinutes], function(err) {

            this.emit('update', candleStickSizeMinutes, completeCandleStick);

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
