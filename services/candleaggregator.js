var _ = require('underscore');
var tools = require('../util/tools.js');
var indicatorMACD = require('../indicators/MACD');

var aggregator = function(indicatorSettings, storage, logger) {

  this.storage = storage;
  this.candleStickSizeMinutesArray = indicatorSettings.candleStickSizeMinutesArray; //now an array
  this.initialCandleDBWriteDone = false;
  this.logger = logger;
  this.previousCompleteCandleStick = [];//an array of objects
  this.MACD = new indicatorMACD(indicatorSettings, logger);

/*
  for(var i = 0; i<this.candleStickSizeMinutesArray.length; i++){
    this.previousCompleteCandleStickPeriod[ this.candleStickSizeMinutesArray[i] ]['period'] = 0;
  }
*/
  _.bindAll(this, 'update', 'setCandleStickSize', 'createInitialIndicatorCandles', 'processInitialMultiCandleUpdate');

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

      //this.logger.log('** Complete Candelstick: '+JSON.stringify(completeCandleStick));
      if(completeCandleStick) {

        if(!this.previousCompleteCandleStick[candleStickSizeMinutes]) {

          this.previousCompleteCandleStick[candleStickSizeMinutes] = completeCandleStick; //save whole thing to do MACD comparison w/o another lookup

        }

        this.logger.log('***** Complete Candelstick Period: '+completeCandleStick.period+' | previousCompleteCandleStickPeriod: '+this.previousCompleteCandleStick[ candleStickSizeMinutes ].period) 
        if(completeCandleStick.period !== this.previousCompleteCandleStick[ candleStickSizeMinutes ].period) {

          this.logger.log('\n\n\n *** Created a new '+ candleStickSizeMinutes +'min candlestick! *** \n\n\n');

          var indicator = this.MACD.calculateFromCandles(candleStickSizeMinutes, completeCandleStick);

          _.extend(completeCandleStick[candleStickSizeMinutes+'min'], indicator);

          //this.logger.log('JSON: '+JSON.stringify(completeCandleStick));

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
                //this.logger.log('\n\n\n\n'+JSON.stringify(candleSticks));

                var latestCandleStick = candleSticks[0];
                this.emit('update', candleStickSizeMinutes, latestCandleStick);

              }.bind(this));

            }

          }.bind(this));

          this.previousCompleteCandleStick[candleStickSizeMinutes] = completeCandleStick;

          this.storage.removeOldDBCandles(candleStickSizeMinutes, function(err) {

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

aggregator.prototype.createInitialIndicatorCandles = function(initialCandles) {
  console.log('\n\n\Candleaggregator | createInitialIndicatorCandles\nfrom '+initialCandles.length+' candles...');
  console.log('InitialCandles[0].period: '+initialCandles[0].period+' | initialCandles['+((initialCandles.length)-1)+'].period: '+initialCandles[initialCandles.length-1].period);

  var aggregatedCandleSticks = {};
  for(var i = 0; i<this.candleStickSizeMinutesArray.length; i++){

    console.log('for '+this.candleStickSizeMinutesArray[i]+'min');

    aggregatedCandleSticks[ this.candleStickSizeMinutesArray[i] ] = this.storage.aggregateCandleSticks2(this.candleStickSizeMinutesArray[i], initialCandles);

    console.log('\nStorage | createInitialIndicatorCandles\naggregatedCandleSticks[5]: '+JSON.stringify(aggregatedCandleSticks[ this.candleStickSizeMinutesArray[i] ]));
  }

  this.storage.pushBulkMultiCandles(aggregatedCandleSticks, this.processInitialMultiCandleUpdate);

};

aggregator.prototype.processInitialMultiCandleUpdate = function(err, multiCandlesArray) {

  if(err) {

    var parsedError = JSON.stringify(err);

    if(err.stack) {
      parsedError = err.stack;
    }

    this.logger.error('Couldn\'t create candlesticks due to a database error');
    this.logger.error(parsedError);

    process.exit();

  } else {
    //console.log('candleStickSizeMinutes: '+candleStickSizeMinutes);
    this.storage.getLastNCandles(this.candleStickSizeMinutesArray[ this.candleStickSizeMinutesArray.length-1 ], 1, function(err, candleSticks) {

      var latestCandleStick = candleSticks[0];
      console.log('this.initialCandleDBWriteDone: '+this.initialCandleDBWriteDone);

      if(!this.initialCandleDBWriteDone) {

        this.initialCandleDBWriteDone = true;
        console.log('this.initialCandleDBWriteDone: '+this.initialCandleDBWriteDone);
        this.emit('initialCandleDBWrite');

      } else {

        console.log('\n\candleAggregator | processInitialMultiCandleUpdate\nlatestCandleStick: '+latestCandleStick);

        this.emit('update', latestCandleStick);

      }

    }.bind(this));

  }

};

module.exports = aggregator;
