var _ = require('underscore');
var async = require('async');
var tools = require('../util/tools.js');
var indicatorMACD = require('../indicators/MACD');

var insights = function(indicatorSettings, storage, logger) {

  this.storage = storage;
  this.logger = logger;
  this.indicator = {};
  this.MACD = new indicatorMACD(indicatorSettings.options, logger);

   _.bindAll(this, 'update');

};

//---EventEmitter Setup
var Util = require('util');
var EventEmitter = require('events').EventEmitter;
Util.inherits(insights, EventEmitter);
//---EventEmitter Setup

insights.prototype.update = function(callback) {
  this.logger.log('Updating indicators...'); 

  this.storage.getLastNTicks(100, function(err, ticks) {
  	for(var i = 0; i < ticks.length; i++){
  		this.indicator = this.MACD.calculateFromTick(ticks[i]).indicator;
  		if(this.indicator !== null)
  			this.logger.log('Close: '+this.indicator.close+' | MACD: '+this.indicator.macd+' | emaLong: '+this.indicator.emaLong+' | emaShort: '+this.indicator.emaShort+' | macdSignal: '+this.indicator.macdSignal+' | Histogram: '+this.indicator.macdHistogram);
  			//this.logger.log('MACD '+i+': '+this.MACD.calculateFromTick(ticks[i]).indicator.macd);
  	}
  }.bind(this));
}
module.exports = insights;
