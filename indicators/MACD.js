var _ = require('underscore');
var tools = require('../util/tools.js');

var indicator = function(indicatorSettings, logger) {

  this.options = indicatorSettings.options;
  this.candleStickSizeMinutesArray = indicatorSettings.candleStickSizeMinutesArray;
  this.logger = logger;
  this.position = {};
  this.indicators = [];
  this.previousIndicator = {};
  this.advice = 'hold';
  this.length = 0;

  _.bindAll(this, 'calculate', 'calculateFromCandles', 'setPosition');

  if(!'neededPeriods' in this.options || !'longPeriods' in this.options || !'shortPeriods' in this.options || !'emaPeriods' in this.options || !'buyThreshold' in this.options || !'sellThreshold' in this.options) {
    var err = new Error('Invalid options for MACD indicator, exiting.');
    this.logger.error(err.stack);
    process.exit();
  }

  //pre-populate indicator objects into array
  for ( var i = 0; i < this.candleStickSizeMinutesArray.length; i++){
    this.indicators[ this.candleStickSizeMinutesArray[i] ] = {'length' : 0};
  }

  //don't forget the 1min outside of the array
  this.indicators[1] = {'length' : 0};

  // indicatorOptions
  // options: {neededPeriods: number, longPeriods: number, shortPeriods: number, emaPeriods: number, buyThreshold: number, sellThreshold: number}

};

//-------------------------------------------------------------------------------HelperFunctions
var calculateEma = function(periods, priceToday, previousEma) {

  if(!previousEma) {
    previousEma = priceToday;
  }

  var k = 2 / (periods + 1);
  var ema = (priceToday * k) + (previousEma * (1 - k));

  return ema;

};
//-------------------------------------------------------------------------------HelperFunctions

indicator.prototype.calculate = function(cs) {

  this.length += 1;
  this.previousIndicator = this.indicator;

  var usePrice = cs.close;

  var emaLong = calculateEma(this.options.longPeriods, usePrice, this.previousIndicator.emaLong);
  var emaShort = calculateEma(this.options.shortPeriods, usePrice, this.previousIndicator.emaShort);

  var macd = emaShort - emaLong;
  var macdSignal = calculateEma(this.options.emaPeriods, macd, this.previousIndicator.macdSignal);
  var macdHistogram = tools.round(macd - macdSignal, 8);

  this.indicator = {'emaLong': emaLong, 'emaShort': emaShort, 'macd': macd, 'macdSignal': macdSignal, 'result': macdHistogram};
  console.log(JSON.stringify(this.indicator));

  if(this.previousIndicator.result <= this.options.buyThreshold && this.indicator.result > this.options.buyThreshold) {

    this.advice = 'buy';

  } else if(this.previousIndicator.result >= this.options.sellThreshold && this.indicator.result < this.options.sellThreshold) {

    this.advice = 'sell';

  } else {

    this.advice = 'hold';

  }

  if(this.length >= this.options.neededPeriods) {

    return {advice: this.advice, indicatorValue: this.indicator.result};

  } else {

    return {advice: 'hold', indicatorValue: null};

  }

};

indicator.prototype.calculateFromCandles = function(candleStickSizeMinute, cs) {
  this.indicators[candleStickSizeMinute].length++;
  //console.log('indicator['+candleStickSizeMinute+']: '+JSON.stringify(this.indicators[candleStickSizeMinute]) );

  var previousIndicator = this.indicators[candleStickSizeMinute],
      usePrice = cs.close;

  var emaLong = calculateEma(this.options.longPeriods, usePrice, previousIndicator.emaLong),
      emaShort = calculateEma(this.options.shortPeriods, usePrice, previousIndicator.emaShort);

  var macd = emaShort - emaLong,
      macdSignal = calculateEma(this.options.emaPeriods, macd, previousIndicator.macdSignal),
      macdHistogram = tools.round(macd - macdSignal, 8);


  this.indicators[candleStickSizeMinute] = {'emaLong': emaLong, 'emaShort': emaShort, 'macd': macd, 'macdSignal': macdSignal, 'macdHistogram': macdHistogram, 'length': this.indicators[candleStickSizeMinute].length};
  
  if(this.indicators[candleStickSizeMinute].length >= this.options.neededPeriods) {
    return this.indicators[candleStickSizeMinute];
  } else {
    //console.log('****** Not enough periods: '+this.indicators[candleStickSizeMinute].length+'/'+this.options.neededPeriods);
    return {'emaLong': null, 'emaShort': null, 'macd': null, 'macdSignal': null, 'macdHistogram': null};
  }

}

indicator.prototype.calculateFromTick = function(tick) {

  this.length += 1;
  this.previousIndicator = this.indicator;

  var usePrice = tick.price;

  var emaLong = calculateEma(this.options.longPeriods, usePrice, this.previousIndicator.emaLong);
  var emaShort = calculateEma(this.options.shortPeriods, usePrice, this.previousIndicator.emaShort);

  var macd = emaShort - emaLong;
  var macdSignal = calculateEma(this.options.emaPeriods, macd, this.previousIndicator.macdSignal);
  var macdHistogram = tools.round(macd - macdSignal, 8);

  this.indicator = {'close': usePrice, 'emaLong': emaLong, 'emaShort': emaShort, 'macd': macd, 'macdSignal': macdSignal, 'macdHistogram': macdHistogram};

  //this.logger.log(tick.price+' | '+emaLong+' | '+emaShort+' | '+macd+' | '+macdSignal+' | '+macdHistogram);

  if(this.length >= this.options.neededPeriods) {

    return this.indicator;

  } else {

    return {'close': usePrice, 'emaLong': emaLong, 'emaShort': emaShort, 'macd': null, 'macdSignal': null, 'macdHistogram': null};;

  }

};

indicator.prototype.calculateFrom = function(data) {

  this.length += 1;
  this.previousIndicator = this.indicator;

  var usePrice = tick.price;

  var emaLong = calculateEma(this.options.longPeriods, usePrice, this.previousIndicator.emaLong);
  var emaShort = calculateEma(this.options.shortPeriods, usePrice, this.previousIndicator.emaShort);

  var macd = emaShort - emaLong;
  var macdSignal = calculateEma(this.options.emaPeriods, macd, this.previousIndicator.macdSignal);
  var macdHistogram = tools.round(macd - macdSignal, 8);

  this.indicator = {'close': usePrice, 'emaLong': emaLong, 'emaShort': emaShort, 'macd': macd, 'macdSignal': macdSignal, 'macdHistogram': macdHistogram};

  //this.logger.log(tick.price+' | '+emaLong+' | '+emaShort+' | '+macd+' | '+macdSignal+' | '+macdHistogram);

  if(this.length >= this.options.neededPeriods) {

    return this.indicator;

  } else {

    return {'close': usePrice, 'emaLong': emaLong, 'emaShort': emaShort, 'macd': null, 'macdSignal': null, 'macdHistogram': null};;

  }

};

indicator.prototype.setPosition = function(pos) {

  this.position = pos;

};

module.exports = indicator;
