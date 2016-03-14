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
  _.bindAll(this, 'update', 'setCandleStickSize', 'processInitalCandleUpdate');

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

  for(var i = 0; i<this.candleStickSizeMinutesArray.length; i++){

    console.log('for '+this.candleStickSizeMinutesArray[i]+'min');

    var aggregatedCandleSticks = this.storage.aggregateCandleSticks2(this.candleStickSizeMinutesArray[i], initialCandles);

    //console.log('\n\n\n\n\n aggregatedCandleSticks \n'+JSON.stringify(aggregatedCandleSticks));
    if( aggregatedCandleSticks.length > 1){
      this.storage.pushBulk(this.candleStickSizeMinutesArray[i], aggregatedCandleSticks, this.processInitalCandleUpdate);
    }
  }

};

aggregator.prototype.processInitalCandleUpdate = function(err, initialCandles, candleStickSizeMinutes) {

  if(err) {

    var parsedError = JSON.stringify(err);

    if(err.stack) {
      parsedError = err.stack;
    }

    this.logger.error('Couldn\'t create candlesticks due to a database error');
    this.logger.error(parsedError);

    process.exit();

  } else {
    console.log('candleStickSizeMinutes: '+candleStickSizeMinutes);
    this.storage.getLastNCandles(candleStickSizeMinutes, 1, function(err, candleSticks) {

      var latestCandleStick = candleSticks[0];
      console.log('this.initialCandleDBWriteDone: '+this.initialCandleDBWriteDone);
      if(!this.initialCandleDBWriteDone) {

        this.emit('initialCandleDBWrite', initialCandles);
        this.initialCandleDBWriteDone = true;

      } else {

        console.log('\n\n\n\n\nlatestCandleStick: '+latestCandleStick);

        this.emit('update', latestCandleStick);

      }

    }.bind(this));

  }

};

module.exports = aggregator;
