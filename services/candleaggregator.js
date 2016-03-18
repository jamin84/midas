var _ = require('underscore');
var tools = require('../util/tools.js');
var extend = require('util')._extend;
var indicatorMACD = require('../indicators/MACD');

var aggregator = function(indicatorSettings, storage, logger) {

  this.storage = storage;
  this.candleStickSizeMinutesArray = indicatorSettings.candleStickSizeMinutesArray; //now an array
  this.initialCandleDBWriteDone = false;
  this.lastCandleStored = [];
  this.logger = logger;
  this.previousCompleteCandleStick = [];//an array of objects
  this.MACD = new indicatorMACD(indicatorSettings, logger);

  //initialize array
  for(var i = 0; i<this.candleStickSizeMinutesArray.length; i++){
    this.lastCandleStored[ this.candleStickSizeMinutesArray[i] ] = false;
  }

  _.bindAll(this, 'update', 'setCandleStickSize', 'updateIndicatorCandles', 'aggregateCandleSticks', 'updateCrossovers', 'processBulkCandleUpdate', 'processMultiCandleUpdate');

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


aggregator.prototype.createCandleSticks2 = function(candleStickSizeMinutes, ticks, callback) {
  //goal: to create 1 min candle sticks on whole time blocks, e.g. 8:00:00pm or 9:15:00pm
  //TODO: account for periods of no trading.
  //TODO: account for ticks that happen before the candleStickSizeMinutes period e.g we're still within the next period after lastStoragePeriod

  if(ticks.length > 0) {

    this.storage.getLastNonEmptyPeriod(candleStickSizeMinutes, function(err, lastStoragePeriod) {

      //lastStorage period == 0 (no previous candles), or unix time stamp of last 1min candle

      //this.storage.getLastNonEmptyClose(candleStickSizeMinutes, function(err, lastNonEmptyClose) {

        var candleStickSizeSeconds = candleStickSizeMinutes*60,
            toBePushed = [], //array of candles to bulk push
            indicator = {}, //macd indicator
            previousClose = 0,
            beginTimeStamp = 0,
            endTimeStamp = 0, //just for convention sake, its really = candleTimePeriod
            candleTimePeriod = 0,
            candleTimePeriodString = candleStickSizeMinutes+'min',
            currentCandleStick = {},
            candleStickInfoDefaults = {'open':0,'high':0,'low':0,'close':0,'volume':0,'vwap':0,'numTrades': 0,'macd': 0,'macdSignal': 0,'macdHistogram': 0},
            candleStickInfo = extend({}, candleStickInfoDefaults);
            candleStickInfo = extend(candleStickInfo, {'open':ticks[0].price, 'low':ticks[0].price})
            cumuPV = 0,
            cumuV = 0,
            previousCandle = {};

        if( lastStoragePeriod > 0 && ticks[0].date > lastStoragePeriod ){
          //if we have previous candles and this tick its outside of it
          candleTimePeriod = lastStoragePeriod + candleStickSizeSeconds;

        } else if( lastStoragePeriod > 0 && ticks[0].date < lastStoragePeriod ) {
          //if we have previous candles and this tick needs to be added to the lastStoragePeriod
          candleTimePeriod = lastStoragePeriod;
        } else {
          candleTimePeriod = (Math.floor((ticks[0].date+candleStickSizeSeconds)/candleStickSizeSeconds)*candleStickSizeSeconds);
        }

        beginTimeStamp = candleTimePeriod - candleStickSizeSeconds;
        endTimeStamp = candleTimePeriod;

        //console.log('\n\nDataprocessor | createCandleSticks2\nlastStoragePeriod: '+lastStoragePeriod+' | candleTimePeriod: '+candleTimePeriod+' | beginTimeStamp: '+beginTimeStamp+' | endTimeStamp: '+endTimeStamp+' | previousCandle.period: '+previousCandle.period);
        //console.log('ticks[0].date: '+ticks[0].date+' | ticks.length: '+ticks.length);

        currentCandleStick = {'period':candleTimePeriod}; 

        //console.log('\ncurrentCandleStick: '+JSON.stringify(currentCandleStick)+'\n');

        ticks.forEach(function(tick, i) {
          //console.log('candleTimePeriod: '+candleTimePeriod);
          //console.log('Tick: '+JSON.stringify(tick));

          //console.log('setting candle info...');
          candleStickInfo.volume += tick.amount;
          candleStickInfo.numTrades++;
          candleStickInfo.high = (tick.price > candleStickInfo.high ? tick.price : candleStickInfo.high);
          candleStickInfo.low = (tick.price < candleStickInfo.low ? tick.price : candleStickInfo.low);
          candleStickInfo.close = tick.price;

          //for VWAP calculations
          cumuV += tick.amount;

          if( tick.date < candleTimePeriod && i < ticks.length){
            //this means we're in the process of updating a candle, retrieving new ticks and updating until a new period
            //console.log('\ntick.date < candleTimePeriod');

            //currently we aggregate after the conditional, adding the complete candle to the toBePushed array. After all the ticks are looped we push the array.

          } else {
            //console.log('\nNEW period...');
         
            //console.log('\nNEW currentCandleStick: '+JSON.stringify(currentCandleStick)+'\n');

            //tick aggregation for this candle complete (excluding current tick)

            var typicalPrice = (candleStickInfo.high + candleStickInfo.low+candleStickInfo.close)/3,
                pv = typicalPrice * candleStickInfo.volume;

            cumuPV += pv;

            candleStickInfo.vwap = tools.round(cumuPV / cumuV, 8);
            
            //calculate MACD (this function calculates from the candles, stores each by the min, and returns the calculations once it's reached the config threshold for periods)           
            indicator = this.MACD.calculateFromCandles(candleStickSizeMinutes, candleStickInfo);
            candleStickInfo = extend(candleStickInfo, indicator);
            //console.log('MACD: '+JSON.stringify(indicator));

            currentCandleStick[ candleTimePeriodString ] = candleStickInfo;

            toBePushed.push(currentCandleStick);
            //console.log('\nPUSHED: '+JSON.stringify(currentCandleStick)+'\n');

            candleTimePeriod += candleStickSizeSeconds;

            //update time period and reset values for this next candle
            currentCandleStick = {'period':candleTimePeriod};

            candleStickInfo = extend({}, candleStickInfoDefaults);   
            candleStickInfo = extend(candleStickInfo, {'open':tick.price, 'low':tick.price});

            if( tick.date > candleTimePeriod ){ //if this next tick is outside consecutive period, push previous and increase
              //console.log('TICK OUTSIDE RANGE...');
              //console.log('tick.date: '+tick.date+' > candleTimePeriod: '+candleTimePeriod);
              //console.log('past the next candleTimePeriod, adding interim candle...');
              //if there is a gap between candle periods, set the last candle to the previous and then update the period to match the tick
            
              previousCandle = {'period':candleTimePeriod}; //update previous candle object
              candleStickInfo = {'open':previousClose,'high':previousClose,'low':previousClose,'close':previousClose,'volume':0,'vwap':previousClose,'numTrades': 0,'macd': 0,'macdSignal': 0,'macdHistogram': 0};

              indicator = this.MACD.calculateFromCandles(candleStickSizeMinutes, candleStickInfo);
              candleStickInfo = extend(candleStickInfo, indicator);
              
              previousCandle[ candleTimePeriodString ] = candleStickInfo;
              toBePushed.push(previousCandle);
              //console.log('PUSHED previous candle: '+JSON.stringify(previousCandle));

              candleTimePeriod += candleStickSizeSeconds;
              currentCandleStick = {'period':candleTimePeriod};
              //console.log('updating to period '+candleTimePeriod+'...');
              //TODO: loop and insert N number of appropriate candles for the time lap
              //TODO: set up a check that ensures the ticks arent actually in DB, and if they are/can be downloaded, to update the candles

              //use this to calculate the # of loops between the candles 
              //candleTimePeriod = (Math.floor((tick.date+candleStickSizeSeconds)/candleStickSizeSeconds)*candleStickSizeSeconds);

            }

          }

          previousClose = tick.price;

        }.bind(this));
        
        //console.log('toBePushed.length: '+toBePushed.length);
        //console.log('\ntoBePushed: '+JSON.stringify(toBePushed));

        if (toBePushed.length > 0){
          this.storage.pushBulk(candleStickSizeMinutes, toBePushed, callback);
        }

     // }.bind(this));
    }.bind(this));
  }
}


aggregator.prototype.updateIndicatorCandles = function(index) {
  console.log('\n\n\Candleaggregator | updateIndicatorCandles');
  var aggregatedCandleSticks = {},
      candleStickSize = (index ? this.candleStickSizeMinutesArray[index] : this.candleStickSizeMinutesArray[0]);

  //for(var i = 0; i<this.candleStickSizeMinutesArray.length; i++){
    //trying to avoid the loop, so start with the first one

    //1b. find Highest Common Denominator (HCD) for indicator
    var index = this.candleStickSizeMinutesArray.indexOf(candleStickSize),
        pCandleStickSize = (index == 0 ? 1 : this.candleStickSizeMinutesArray[ index-1 ]),
        HCD = (candleStickSize % pCandleStickSize == 0 ? candleStickSize / pCandleStickSize : '1');

    //1a. get Latest Indicator Candle period (LIP)
    this.storage.getLastNonEmptyPeriod(candleStickSize, function(err, lastStoragePeriod) {
      //console.log('\nindex: '+index+' | candleStickSize: '+candleStickSize+' | lastStoragePeriod: '+JSON.stringify(lastStoragePeriod));

      //1c. get all HCD candles since LIP
      this.storage.getAllCandlesSince(pCandleStickSize, lastStoragePeriod, function(err, candleSticks){
        console.log('\n getAllCandlesSince.length: '+candleSticks.length);

        //2. aggregate HCD candles for indicator
        if( candleSticks.length > HCD){
          aggregatedCandleSticks = this.aggregateCandleSticks(candleStickSize, candleSticks);

          if( lastStoragePeriod == 0 || this.lastCandleStored[ candleStickSize ] == 0){
            this.lastCandleStored[ candleStickSize ] = aggregatedCandleSticks[0];        
          }

          console.log('\nlastCandleStored: '+JSON.stringify(this.lastCandleStored[ candleStickSize ]) );

          //TODO: 3/17/2016: store MACD info
          //compare previous to current for CROSSOVER
          //if crossover occurs, store current period, type of crossover (1-6), current candlestick size.

          //CROSSOVER

          //1. does the current histogram direction(pos vs neg) match the previous histogram? if not, record.
          //2. does the current macd direction match the previous macd? if not, record
          //3. store by period, like candles {period : 12355, 1min : {}, 5min : {} }

          this.updateCrossovers(aggregatedCandleSticks, candleStickSize);

          this.lastCandleStored[ candleStickSize ] = aggregatedCandleSticks.slice(-1)[0];
          console.log('\nupdateIndicatorCandles | lastCandleStored: '+JSON.stringify(this.lastCandleStored[ candleStickSize ]) );

          //console.log('\nCandleaggregator | updateIndicatorCandles\naggregatedCandleSticks['+candleStickSize+']: '+JSON.stringify(aggregatedCandleSticks[ candleStickSize ]));
          this.storage.pushBulk(candleStickSize, aggregatedCandleSticks, this.processBulkCandleUpdate); //set up next candle update
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

    //console.log('i: '+i+' | candleStick: '+JSON.stringify(candleStick));

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
        candleStickInfo = extend(candleStickInfo, indicator);
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
        var index = this.candleStickSizeMinutesArray.indexOf(candleStickMinutes);
        console.log('\n\candleAggregator | processInitialMultiCandleUpdate\nlatestCandleStick: '+latestCandleStick);

        if( index < this.candleStickSizeMinutesArray.length-1 ){
          updateIndicatorCandles(index+1);
        } else {
          this.emit('update', latestCandleStick);          
        }

      }

    }.bind(this));

  }

};

aggregator.prototype.updateCrossovers = function(candleSticks, candleStickSize) {
  console.log('\ncandleaggregator | updateCrossover\ncandles.length: '+candleSticks.length);
  console.log('\nlastCandleStored: '+JSON.stringify(this.lastCandleStored[candleStickSize]) );

/*
  if( !this.lastCandleStored[ candleStickSize ][ candleStickSize+'min' ].macdHistogram ){
    console.log('Not enough MACD data, returning...')
    return;
  }
*/
  var crossoverArray = [];

  _.each(candleSticks, function(candle, i){
    var initialDirection = 0,
        previousDirection = 0,
        previousCandle = ( i == 0 ? (this.lastCandleStored[ candleStickSize ] ? this.lastCandleStored[ candleStickSize ] : candleSticks[ i ] ) : candleSticks[ i-1 ] ),
        crossover = {},
        candleStickSizeString = candleStickSize+'min',
        signal = 0;

    //console.log('\ncurrentCandle['+candleStickSizeString+']: '+JSON.stringify(candle));
    //console.log('\npreviousCandle['+candleStickSizeString+']: '+JSON.stringify(previousCandle) );
    
    function checkHistogramCrossover(current, previous){
      //find direction (pos or neg of values)
      currentDirection = Math.sign(current);
      previousDirection = Math.sign(previous);

      if( currentDirection > previousDirection ){
        return 1;// from - to + means macd crossed above signal
      } else if( currentDirection < previousDirection ){
        return 2;// from + to - means macd crossed below signal
      }else {
        return false;
      }

    }

    function checkZeroCrossover(current, previous){

      if( previous < 0 && current > 0){
        return 3; //macd crosses below 0 line
      } else if( previous > 0 && current > 0){
        return 4;
      } else {
        return false;
      }

    }

    /* Signals index
      1 - MACD crosses ABOVE signal       (macd > signal now)
      2 - MACD crosses BELOW signal line  (macd < signal now)
      3 - MACD crosses ABOVE zero line    (macd > 0 now)
      4 - MACD crosses BELOW zero line    (macd < 0 now)
      5 - Signal crosses ABOVE zero line  (signal > 0 now)
      6 - Signal crosses BELOW zero line  (signal < 0 now)
    */

    if( signal = checkHistogramCrossover(candle[ candleStickSizeString ].macdHistogram, previousCandle[ candleStickSizeString ].macdHistogram) ){
      console.log('macd/signal (histogram) crossover recorded!');
      crossover = {'period': candle.period};
      crossover[ candleStickSizeString ] = {'type': signal}
      crossoverArray.push(crossover);
    }

    if( signal = checkZeroCrossover(candle[ candleStickSizeString ].macd, previousCandle[ candleStickSizeString ].macd) ){
      console.log('macd / 0 crossover recorded!');
      crossover = {'period': candle.period};
      crossover[ candleStickSizeString ] = {'type': signal}
      crossoverArray.push(crossover);
    }

    if( signal = checkZeroCrossover(candle[ candleStickSizeString ].macdSignal, previousCandle[ candleStickSizeString ].macdSignal) ){
      console.log('macd signal/0 crossover recorded!');
      crossover = {'period': candle.period};
      crossover[ candleStickSizeString ] = {'type': signal+=2}
      crossoverArray.push(crossover);
    }


  }.bind(this));

  this.storage.pushCrossovers(candleStickSize, crossoverArray, this.processCrossovers);
  console.log('this.lastCandleStored: '+JSON.stringify(candleSticks.slice(-1)[0]));
  this.lastCandleStored[ candleStickSize ] = candleSticks.slice(-1)[0];

}

aggregator.prototype.processCrossovers = function(err, multiCandlesArray) {

  if(err) {

    var parsedError = JSON.stringify(err);

    if(err.stack) {
      parsedError = err.stack;
    }

    this.logger.error('Couldn\'t create candlesticks due to a database error');
    this.logger.error(parsedError);

    process.exit();

  } else {

    console.log('\n\candleAggregator | processCrossovers\nDone!');
    this.emit('updatedCrossovers');

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
