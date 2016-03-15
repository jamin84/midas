var _ = require('underscore');
var async = require('async');
var tools = require('../util/tools.js');
var extend = require('util')._extend;
var indicatorMACD = require('../indicators/MACD');

var processor = function(indicatorSettings, storage, logger) {

  this.initialDBWriteDone = false;
  this.initialTickDBWriteDone = false;
  this.candleStickSizeMinutesArray = indicatorSettings.candleStickSizeMinutesArray;
  this.storage = storage;
  this.logger = logger;
  this.MACD = new indicatorMACD(indicatorSettings, logger);

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

processor.prototype.createCandleSticks = function(candleStickSizeMinutes, ticks, callback) {

  if(ticks.length > 0) {

    this.storage.getLastNonEmptyPeriod(candleStickSizeMinutes, function(err, lastStoragePeriod) {

      this.storage.getLastNonEmptyClose(candleStickSizeMinutes, function(err, lastNonEmptyClose) {

        var candleStickSizeSeconds = 60;

        var toBePushed = [];

        var indicator = {};

        var previousClose = lastNonEmptyClose;

        var tickTimeStamp = ticks[0].date;

        //I don't agree with this calcuation
        var firstTickCandleStick = (Math.floor(ticks[0].date/candleStickSizeSeconds)*candleStickSizeSeconds);

        if(lastStoragePeriod < firstTickCandleStick && lastStoragePeriod !== 0) {
          tickTimeStamp = lastStoragePeriod + candleStickSizeSeconds;
        }

        var now = tools.unixTimeStamp(new Date().getTime());

        var startTimeStamp = (Math.floor(tickTimeStamp/candleStickSizeSeconds)*candleStickSizeSeconds);
        var stopTimeStamp = (Math.floor(now/candleStickSizeSeconds)*candleStickSizeSeconds);

        var endTimeStamp = startTimeStamp + candleStickSizeSeconds;

        while(endTimeStamp < ticks[0].date) {

          toBePushed.push({'period':startTimeStamp, '1min': {'open':previousClose,'high':previousClose,'low':previousClose,'close':previousClose,'volume':0, 'vwap':previousClose, 'trades' : 0, 'macd': undefined, 'macdSignal': undefined, 'macdHistogram': undefined}});

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

processor.prototype.processUpdate = function(err, initialCandles, candleStickSizeMinutes) {
  //console.log('\n\n\n\nPROCESSOR: '+JSON.stringify(initialCandles));

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
      //console.log('\n\n\n\n\nthis.initialDBWriteDone: '+this.initialDBWriteDone)
      if(!this.initialDBWriteDone) {

        this.emit('initialDBWrite', initialCandles);
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

      if(!this.initialTickDBWriteDone) {

        this.emit('initialTickDBWrite');
        this.initialTickDBWriteDone = true;

      } else {

        this.emit('update', latestTick);

      }

    }.bind(this));

  }

};


processor.prototype.createCandleSticks2 = function(candleStickSizeMinutes, ticks, callback) {
  //goal: to create 1 min candle sticks on whole time blocks, e.g. 8:00:00pm or 9:15:00pm
  //TODO: account for periods of no trading.
  //TODO: account for ticks that happen before the candleStickSizeMinutes period e.g we're still within the next period after lastStoragePeriod

  if(ticks.length > 0) {

    this.storage.getLastNonEmptyPeriod(candleStickSizeMinutes, function(err, lastStoragePeriod) {

      //lastStorage period == 0 (no previous candles), or unix time stamp of last 1min candle

      this.storage.getLastNonEmptyClose(candleStickSizeMinutes, function(err, lastNonEmptyClose) {

        //lastNonEmptyClose == ?

        var candleStickSizeSeconds = candleStickSizeMinutes*60,
            toBePushed = [], //array of candles to bulk push
            indicator = {}, //macd indicator
            previousClose = lastNonEmptyClose,
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
            previousCandle = lastNonEmptyClose[0] || {};


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

        console.log('\n\nDataprocessor | createCandleSticks2\nlastStoragePeriod: '+lastStoragePeriod+' | candleTimePeriod: '+candleTimePeriod+' | beginTimeStamp: '+beginTimeStamp+' | endTimeStamp: '+endTimeStamp+' | previousCandle.period: '+previousCandle.period);
        console.log('ticks[0].date: '+ticks[0].date+' | ticks.length: '+ticks.length);

        currentCandleStick = {'period':candleTimePeriod}; 

        console.log('\ncurrentCandleStick: '+JSON.stringify(currentCandleStick)+'\n');

        ticks.forEach(function(tick) {
          console.log('candleTimePeriod: '+candleTimePeriod);
          //TODO account for bigger gaps in trade times
          if( tick.date < candleTimePeriod){
            //this means we're in the process of updating a candle, retriever new ticks and updating until a new period
            //console.log('\ntick.date < candleTimePeriod');

            //if lastStoragePeriod > 0, WE NEED TO ADD THIS TICK TO THE EXISTING CANDLE INFO

            //currently we aggregate after the conditional, adding the complete candle to the toBePushed array. After all the ticks are looped we push the array.

            //Should I add the candleStick here but instead of 'pushBulk' use 'push' to update the existing data as new ticks come in?
            //Maybe extract the calculations from the below else and make this conditional choose between 'push' and pushBulk' 

          } else {
            //console.log('\nNEW period...');
         
            console.log('\nNEW currentCandleStick: '+JSON.stringify(currentCandleStick)+'\n');

            //tick aggregation for this candle complete (excluding current tick)

            var typicalPrice = (candleStickInfo.high + candleStickInfo.low+candleStickInfo.close)/3,
                pv = typicalPrice * candleStickInfo.volume;

            cumuPV += pv;

            currentCandleStick[ candleTimePeriodString ].vwap = tools.round(cumuPV / cumuV, 8);
            
            //calculate MACD (this function calculates from the candles, stores each by the min, and returns the calculations once it's reached the config threshold for periods)           
            indicator = this.MACD.calculateFromCandles(candleStickSizeMinutes, candleStickInfo);
            candleStickInfo = extend(candleStickInfo, indicator);
            //console.log('MACD: '+JSON.stringify(indicator));

            toBePushed.push(currentCandleStick);
            console.log('\nPUSHED: '+JSON.stringify(currentCandleStick)+'\n');

            candleTimePeriod += candleStickSizeSeconds;
            previousCandle = extend({}, currentCandleStick);

            //update time period and reset values for this next candle
            currentCandleStick = {'period':candleTimePeriod};

            candleStickInfo = extend({}, candleStickInfoDefaults);   
            candleStickInfo = extend(candleStickInfo, {'open':tick.price, 'low':tick.price});

            if( tick.date > candleTimePeriod ){ //if this next tick is outside consecutive period, push previous and increase
              console.log('TICK OUTSIDE RANGE...');
              console.log('tick.date: '+tick.date+' > candleTimePeriod: '+candleTimePeriod);
              //console.log('past the next candleTimePeriod, adding interim candle...');
              //if there is a gap between candle periods, set the last candle to the previous and then update the period to match the tick
            
              previousCandle.period = candleTimePeriod; //update previous candle object
              toBePushed.push(previousCandle);
              console.log('PUSHED previous candle: '+JSON.stringify(previousCandle));
              console.log('\n***********\ntoBePushed: '+JSON.stringify(toBePushed)+'\n***********\n');

              candleTimePeriod += candleStickSizeSeconds;
              currentCandleStick = {'period':candleTimePeriod};
              console.log('updating to period '+candleTimePeriod+'...');
              //TODO: loop and insert N number of appropriate candles for the time lap
              //TODO: set up a check that ensures the ticks arent actually in DB, and if they are/can be downloaded, to update the candles

              //use this to calculate the # of loops between the candles 
              //candleTimePeriod = (Math.floor((tick.date+candleStickSizeSeconds)/candleStickSizeSeconds)*candleStickSizeSeconds);

            } 

          }
          //console.log('setting candle info...');
          candleStickInfo.volume += tick.amount;
          candleStickInfo.numTrades++;
          candleStickInfo.high = (tick.price > candleStickInfo.high ? tick.price : candleStickInfo.high);
          candleStickInfo.low = (tick.price < candleStickInfo.low ? tick.price : candleStickInfo.low);
          candleStickInfo.close = tick.price;

          //for VWAP calculations
          cumuV += tick.amount;

          currentCandleStick[ candleTimePeriodString ] = candleStickInfo;

          //console.log('Tick: '+JSON.stringify(tick));


        }.bind(this));
        
        console.log('toBePushed.length: '+toBePushed.length);
        //console.log('\ntoBePushed: '+JSON.stringify(toBePushed));

        if (toBePushed.length > 0){
          this.storage.pushBulk(candleStickSizeMinutes, toBePushed, callback);
        }

      }.bind(this));
    }.bind(this));
  }
}

processor.prototype.updateCandleDB = function(candleStickSizeMinutes, ticks) {

  this.storage.getLastNonEmptyPeriod(candleStickSizeMinutes, function(err, lastStoragePeriod) {
    //this.logger.log('\n ticks[0].period: '+ticks[0].date+' | period: '+period );
    var newTicks = _.filter(ticks,function(tick){

      return tick.date >= lastStoragePeriod;

    });
    //this.logger.log('\n newTicks[0].period: '+newTicks[0].date);

    //so we always call this and keep updating the candles as new ticks come in...
    this.createCandleSticks2(candleStickSizeMinutes, newTicks, this.processUpdate);

  }.bind(this));

};

module.exports = processor;