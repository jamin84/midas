var _ = require('underscore');
var tools = require('../util/tools.js');
var extend = require('util')._extend;
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
  _.bindAll(this, 'update', 'setCandleStickSize', 'updateIndicatorCandles', 'processBulkCandleUpdate', 'processMultiCandleUpdate');

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

aggregator.prototype.updateIndicatorCandles = function() {
  console.log('\n\n\Candleaggregator | updateIndicatorCandles');
  var aggregatedCandleSticks = {},
      candleStickSize = 1;

  //for(var i = 0; i<this.candleStickSizeMinutesArray.length; i++){
    //trying to avoid the loop, so start with the first one
    candleStickSize = this.candleStickSizeMinutesArray[0];

    //1b. find Highest Common Denominator (HCD) for indicator
    var index = this.candleStickSizeMinutesArray.indexOf(candleStickSize),
        pCandleStickSize = (index == 0 ? 1 : this.candleStickSizeMinutesArray[ index-1 ]),
        HCD = (candleStickSize % pCandleStickSize == 0 ? candleStickSize / pCandleStickSize : '1');

    //1a. get Latest Indicator Candle period (LIP)
    this.storage.getLastNonEmptyPeriod(candleStickSize, function(err, lastStoragePeriod) {
      console.log('\nindex: '+index+' | candleStickSize: '+candleStickSize+' | lastStoragePeriod: '+JSON.stringify(lastStoragePeriod));
    
      //1c. get all HCD candles since LIP
      this.storage.getAllCandlesSince(pCandleStickSize, lastStoragePeriod, function(err, candleSticks){
        console.log('\n getAllCandlesSince.length: '+candleSticks.length);

        //2. aggregate HCD candles for indicator
        if( candleSticks.length > HCD){
          aggregatedCandleSticks = this.aggregateCandleSticks(candleStickSize, candleSticks);
          console.log('\nCandleaggregator | updateIndicatorCandles\naggregatedCandleSticks['+candleStickSize+']: '+JSON.stringify(aggregatedCandleSticks[ candleStickSize ]));
          this.storage.pushBulk(candleStickSize, aggregatedCandleSticks, this.processBulkCandleUpdate);
        }
  
      }.bind(this));

    }.bind(this));

  //}
  
  //this.storage.pushBulkMultiCandles(aggregatedCandleSticks, this.processMultiCandleUpdate);

};

aggregator.prototype.aggregateCandleSticks = function(candleStickSize, candleSticks) {
    console.log('\ncandleSticks.length: '+candleSticks.length);

  // find this required candle's best divisor based on previous candle sizes, pCandleSize, from the config settings
  // - e.i if candleStickSize == 5, pCandleSize == 1, candleStickSize == 15, pCandleSize = 5, candleStickSize == 60, pCandleSize = 30
  // see if we can use this pCandleSize to save time calculating so we're not stuck using 1min candles for 12hr periods

  var index = this.candleStickSizeMinutesArray.indexOf(candleStickSize),
      pCandleSize = (index == 0 ? 1 : this.candleStickSizeMinutesArray[ index-1 ]),

      //Finds the # of times to loop using highest common denominator (HCD). e.g if candleStickSize = 15, pCandleStickSize = 5, numToLoop == 3 (we're using 3 5min candles to make one 15min candle)
      HCD = (candleStickSize % pCandleSize == 0 ? candleStickSize / pCandleSize : '1'),
      //check to see if we can use it to aggregate e.i no remainders (currently defaulting to itself if no HCD)
      //TODO: update to actaully find HCD.
      pCandleSizeString = (candleStickSize % pCandleSize == 0 ? pCandleSize+'min' : '1min');    

  //if there are not enough candles for the size, return
  if ( (candleSticks.length * pCandleSize) < candleStickSize){
    console.log('\nNot enough candlesticks to aggregate for '+candleStickSize+'min candles');
    return [];
  }

  var i = 1, //iterator for looping to make new candles
      candleStickSizeString = candleStickSize+'min',
      currentCandleStick = {},
      candleStickInfoDefaults = {'open':0,'high':0,'low':0,'close':0,'volume':0,'vwap':0,'numTrades': 0,'macd': 0,'macdSignal': 0,'macdHistogram': 0},
      candleStickInfo = extend({}, candleStickInfoDefaults);
      candleStickInfo = extend(candleStickInfo, {'open':candleSticks[0].price, 'low':candleSticks[0].price}),
      relevantSticks = [],
      aggregatedCandleSticks = [];

  var candleStickSizeSeconds = candleStickSize*60,
      candleTimePeriod = (Math.floor((candleSticks[0].period+candleStickSizeSeconds)/candleStickSizeSeconds)*candleStickSizeSeconds);
      beginTimeStamp = candleTimePeriod - candleStickSizeSeconds;
      endTimeStamp = candleTimePeriod;

  //do we have enough candles to populate?
  if( beginTimeStamp < candleSticks[0].period ){
    //not enough, so bump up the candleTimePeriod to the next period
    //console.log('adjusting candleTimePeriod... from '+candleTimePeriod+' to '+(candleTimePeriod+candleStickSizeSeconds));
    candleTimePeriod+=candleStickSizeSeconds;
    beginTimeStamp = candleTimePeriod - candleStickSizeSeconds;
  }
  
  currentCandleStick = {'period': candleTimePeriod};

  console.log('beginTimeStamp: '+beginTimeStamp+' | candleTimePeriod: '+candleTimePeriod);
  console.log('HCD: '+HCD+' | pCandleSize: '+pCandleSize+' | pCandleSizeString: '+pCandleSizeString);

  _.each(candleSticks, function(candleStick) {

    console.log('i: '+i+' | candleStick: '+JSON.stringify(candleStick));

    if( candleStick.period > beginTimeStamp ){
      relevantSticks.push(candleStick);

      if( i % HCD == 0){        
        //console.log('i: '+i+' | relevantSticks.length: '+relevantSticks.length+' i % numToLoop('+numToLoop+'): '+i % numToLoop);

        //got all the sticks for this period, aggregate
        candleStickInfo.open = relevantSticks[0][ pCandleSizeString ].open;
        candleStickInfo.high = _.max(relevantSticks, function(relevantStick) { return relevantStick[ pCandleSizeString ].high; })[ pCandleSizeString ].high;
        candleStickInfo.low = _.min(relevantSticks, function(relevantStick) { return relevantStick[ pCandleSizeString ].low; })[ pCandleSizeString ].low;
        candleStickInfo.close = relevantSticks[relevantSticks.length-1][ pCandleSizeString ].close;
        candleStickInfo.numTrades =  _.reduce(relevantSticks, function(memo, entry) { return memo + entry[ pCandleSizeString ].numTrades; }, 0);
        candleStickInfo.volume = tools.round(_.reduce(relevantSticks, function(memo, entry) { return memo + entry[ pCandleSizeString ].volume; }, 0), 8);
        candleStickInfo.vwap = tools.round(_.reduce(relevantSticks, function(memo, entry) { return memo + (entry[ pCandleSizeString ].vwap * entry[ pCandleSizeString ].volume); }, 0) / candleStickInfo.volume, 8);

        var indicator = this.MACD.calculateFromCandles(candleStickSize, candleStickInfo);

        candleStickInfo = extend(candleStickInfo, indicator);/*
        candleStickInfo.macd = indicator.macd;
        candleStickInfo.macdSignal = indicator.macdSignal;
        candleStickInfo.macdHistogram = indicator.macdHistogram;*/

        currentCandleStick[ candleStickSizeString ] = candleStickInfo;
        aggregatedCandleSticks.push(currentCandleStick);
        //console.log('\n**** AggregatedCandleSticks: '+JSON.stringify(currentCandleStick)+'\n');

        //reset relevant sticks and candleStickInfo
        relevantSticks = [];      
        currentCandleStick = {'period': currentCandleStick.period+candleStickSizeSeconds}; 
        candleStickInfo = extend({}, candleStickInfoDefaults);

      }
      i++;
    }

  }.bind(this));

  return aggregatedCandleSticks;

};

aggregator.prototype.processBulkCandleUpdate = function(err, candlesArr, candleStickMinutes) {
  console.log('\ncandleaggregator | processBulkCandleUpdate');
  console.log('candlesArr.length: '+candlesArr.length+' | candleStickMinutes: '+candleStickMinutes);

  if(err) {

    var parsedError = JSON.stringify(err);

    if(err.stack) {
      parsedError = err.stack;
    }

    this.logger.error('Couldn\'t create candlesticks due to a database error');
    this.logger.error(parsedError);

    process.exit();

  } else {
    console.log('candleStickMinutes: '+candleStickMinutes);
    this.storage.getLastNCandles(candleStickMinutes, 1, function(err, candleSticks) {

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

aggregator.prototype.processMultiCandleUpdate = function(err, multiCandlesArray) {

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
      console.log('this.CandleDBWriteDone: '+this.initialCandleDBWriteDone);

      if(!this.initialCandleDBWriteDone) {

        this.initialCandleDBWriteDone = true;
        console.log('this.candleDBWriteDone: '+this.initialCandleDBWriteDone);
        this.emit('candleDBWrite');

      } else {

        console.log('\n\candleAggregator | processMultiCandleUpdate\nlatestCandleStick: '+latestCandleStick);

        this.emit('update', latestCandleStick);

      }

    }.bind(this));

  }

};

module.exports = aggregator;
