var _ = require('underscore');
var tools = require('../util/tools.js');

var loggingservice = require('../services/loggingservice.js');
var storageservice = require('../services/storage.js');
var exchangeapiservice = require('../services/exchangeapi.js');
var dataretriever = require('../services/dataretriever.js');
var dataprocessor = require('../services/dataprocessor.js');
var candleaggregator = require('../services/candleaggregator');
var tradingadvisor = require('../services/tradingadvisor.js');
var tradingagent = require('../services/tradingagent.js');
var pushservice = require('../services/pushservice.js');
var ordermonitor = require('../services/ordermonitor.js');
var profitreporter = require('../services/profitreporter.js');
var insightService = require('../services/insightService.js');

//------------------------------Config
var config = require('../config.js');
//------------------------------Config

//------------------------------InitializeModules
var logger = new loggingservice('trader', config.debug);
var storage = new storageservice(config.exchangeSettings, config.indicatorSettings, config.mongoConnectionString, logger);
var exchangeapi = new exchangeapiservice(config.exchangeSettings, config.apiSettings, logger);
var retriever = new dataretriever(config.downloaderRefreshSeconds, exchangeapi, logger);
var aggregator = new candleaggregator(config.indicatorSettings, storage, logger);
var processor = new dataprocessor(config.indicatorSettings, storage, aggregator, logger);
var advisor = new tradingadvisor(config.indicatorSettings, storage, logger);
var agent = new tradingagent(config.tradingEnabled, config.exchangeSettings, storage, exchangeapi, logger);
var pusher = new pushservice(config.pushOver, logger);
var monitor = new ordermonitor(exchangeapi, logger);
var reporter = new profitreporter(config.exchangeSettings.currencyPair, storage, exchangeapi, logger);
var insights = new insightService(config.indicatorSettings, storage, logger);
//------------------------------InitializeModules

var trader = function() {

  retriever.on('update', function(ticks){

    this.logger.log('Processing trades!');
    processor.updateTickDB(ticks);
    processor.updateCandleDB(ticks); //create 1 min candles
    //insights.update();

  });

  processor.on('initialDBWrite', function(candles){
    this.logger.log('initialDBWrite done!');
    //console.log('\n\n\n\n'+JSON.stringify(initialCandles));
    //reporter.start();
    //advisor.start();

  });

  processor.on('update', function(candles){
    this.logger.log('Processor update...');
    if(candles.length>=1){
      aggregator.updateCrossovers(candles, '1');
    }
    aggregator.updateIndicatorCandles(0); //create the non-1min indicator candles, starting with index 0-based from array
    //aggregator.update();
  });

  aggregator.on('update', function(csMinutes, cs){

  });

  advisor.on('advice', function(result) {

    var indicatorValueStr = result.indicatorValue ? ' (' + result.indicatorValue + ')' : '';
    var adviceStr = result.isStart ? 'Start advice: ' : 'Advice: ';
    this.logger.log(adviceStr + result.advice + indicatorValueStr);

    if (!config.tradeAtStart && result.isStart) {
      return false;
    }

    if(result.advice === 'buy') {

      agent.order(result.advice);

    } else if(result.advice === 'sell') {

      agent.order(result.advice);

    }

  });

  agent.on('realOrder',function(orderDetails){

    if(config.pushOver.enabled) {
      pusher.send('Midas - Order Placed!', 'Placed ' + orderDetails.orderType + ' order: (' + orderDetails.amount + '@' + orderDetails.price + ')', 'magic', 1);
    }

    monitor.add(orderDetails, config.orderKeepAliveMinutes);

  });

  agent.on('simulatedOrder',function(orderDetails){

    if(config.pushOver.enabled) {
      pusher.send('Midas - Order Simulated!', 'Simulated ' + orderDetails.orderType + ' order: (' + orderDetails.amount + '@' + orderDetails.price + ')', 'magic', 1);
    }

    monitor.add(orderDetails, config.orderKeepAliveMinutes);

  });

  monitor.on('filled', function(order) {

    if(order.orderDetails.orderType === 'buy') {

      advisor.setPosition({pos: 'bought', price: order.orderDetails.price});

    } else if(order.orderDetails.orderType === 'sell') {

      advisor.setPosition({pos: 'sold', price: order.orderDetails.price});

    }

    reporter.updateBalance(true, order);

  });

  monitor.on('cancelled', function(order, retry) {

    reporter.updateBalance(false, order);

    if(retry) {

      agent.order(order.orderDetails.orderType);

    }

  });

  reporter.on('report', function(report){

    if(config.pushOver.enabled) {
      pusher.send('Midas - Profit Report!', report, 'magic', 1);
    }

  });

  _.bindAll(this, 'start', 'stop');

};

//---EventEmitter Setup
var Util = require('util');
var EventEmitter = require('events').EventEmitter;
Util.inherits(trader, EventEmitter);
//---EventEmitter Setup

trader.prototype.start = function() {

  retriever.start();

};

trader.prototype.stop = function(cb) {

  retriever.stop();

  monitor.resolvePreviousOrder(function() {
    logger.log('Midas stopped succesfully!');
    cb();
  }.bind(this));

  this.emit('done');

};

var traderApp = new trader();

module.exports = traderApp;
