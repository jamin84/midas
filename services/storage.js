var _ = require('underscore');
var mongo = require('mongojs');
var async = require('async');
var extend = require('util')._extend;
var indicatorMACD = require('../indicators/MACD');
var tools = require('../util/tools.js');

var storage = function(exchangeSettings, indicatorSettings, mongoConnectionString, logger) {
  this.pair = exchangeSettings.currencyPair.pair;
  this.exchange = exchangeSettings.exchange;
  this.exchangeBase = exchangeSettings.exchange + exchangeSettings.currencyPair.pair;
  this.exchangeInfoBase = this.exchangeBase + '_Info';
  this.exchangeTicks = exchangeSettings.exchange+ exchangeSettings.currencyPair.pair + '_Ticks';
  this.candleStickSizeMinutesArray = indicatorSettings.candleStickSizeMinutesArray;
  this.MACD = new indicatorMACD(indicatorSettings, logger);
  this.mongoConnectionString = mongoConnectionString;
  this.logger = logger;

  _.bindAll(this, 'pushTicks', 'push', 'getLastTick', 'getLastNCandles', 'getAllCandles', 'getAllCandlesSince', 'getLastClose', 'getLastNonEmptyPeriod', 'getLastNonEmptyClose', 'getLastNCompleteAggregatedCandleSticks', 'getLastCompleteAggregatedCandleStick', 'getCompleteAggregatedCandleSticks', 'getLastNAggregatedCandleSticks', 'getAggregatedCandleSticks', 'getAggregatedCandleSticksSince', 'calculateAggregatedCandleStick', 'aggregateCandleSticks', 'removeOldDBCandles', 'dropCollection', 'getInitialBalance', 'setInitialBalance');

};

storage.prototype.pushTicks = function(csArray, callback) {

  var csDatastore = mongo(this.mongoConnectionString);
  var csCollection = csDatastore.collection(this.exchangeTicks);

  var bulk = csCollection.initializeOrderedBulkOp();

  _.forEach(csArray, function(cs) {
    bulk.find({date: cs.date}).upsert().updateOne(cs);
  });

  bulk.execute(function(err, res) {

    csDatastore.close();

    if(err) {

      callback(err);

    } else {

      callback(null);

    }

  });

};

storage.prototype.pushBulk = function(candleStickSizeMinutes, csArray, callback) {
  //this.logger.log('* Pushing BULK to '+this.exchangeInfoBase+' | '+candleStickSizeMinutes+'min candle');
  /*
  for(var i=0; i<csArray.length; i++){
    this.logger.log('\n csArray['+i+']: '+JSON.stringify(csArray[i]));
  }
*/

  console.log('\nStorage | pushBulk \ncandleStickSizeMinutes: '+candleStickSizeMinutes+' | csArray.length: '+csArray.length+' | csArray[0].period: '+csArray[0].period+' | csArray['+(csArray.length-1)+'].period: '+csArray[csArray.length-1].period+'\n\n');

  var csDatastore = mongo(this.mongoConnectionString);
  var csCollection = csDatastore.collection(this.exchangeInfoBase);
  var candle =  candleStickSizeMinutes+'min';

  var bulk = csCollection.initializeOrderedBulkOp();


  _.forEach(csArray, function(cs) {
    var set = {};
    set[candle] = cs[candle];
    bulk.find({period: cs.period}).upsert().updateOne({$set : set});
  });

  bulk.execute(function(err, res) {
    csDatastore.close();

    if(err) {

      callback(err, csArray, candleStickSizeMinutes);

    } else {

      callback(null, csArray, candleStickSizeMinutes);

    }

  });

};

storage.prototype.pushBulkMultiCandles = function(multiCandles, callback) {

  console.log('\nStorage | pushBulkMultiCandles');
  //console.log('\nStorage | pushBulk \ncandleStickSizeMinutes: '+candleStickSizeMinutes+' | csArray.length: '+csArray.length+' | csArray[0].period: '+csArray[0].period+' | csArray['+(csArray.length-1)+'].period: '+csArray[csArray.length-1].period+'\n\n');

  var csDatastore = mongo(this.mongoConnectionString);
  var csCollection = csDatastore.collection(this.exchangeInfoBase);
  var candle = '';
  var bulk = csCollection.initializeOrderedBulkOp();

  /*
    multiCandles = {
      '5': [{ period: 1234, '5min': {'open': 1234, 'close': 1234}}, {}, {}... ],
      '15': [{ period: 1234, '5min': {'open': 1234, 'close': 1234}}, {}, {}...]
      ...
    }
  */

  //for each candle (5,15,30...) period,
  //for each candles in that period

  //TODO: see if it's possible to have one obj with a mix of all candle periods and not have to double loop?
  _.each(multiCandles, function(candles, min) {
    var minuteString = min+'min';
    var candle = {};

    console.log('\nmin: '+min);
    
    _.each(candles, function(candle, i){
      var set = {};
      set[minuteString] = candle[ minuteString ];

      console.log('\nin 2nd each... set[minuteString]: '+JSON.stringify(candle));

      bulk.find({period: candle.period}).upsert().update({$set : set});
    });    
  });

  bulk.execute(function(err, res) {
    csDatastore.close();

    if(err) {

      callback(err, multiCandles);

    } else {

      callback(null, multiCandles);

    }

  });

};

storage.prototype.push = function(candleStickSizeMinutes, csObject, callback) {
  this.logger.log('\nStorage | push\nPushing to '+this.exchangeInfoBase+' at period: '+csObject.period+' for '+candleStickSizeMinutes+'min candle');

  var csDatastore = mongo(this.mongoConnectionString);
  var csCollection = csDatastore.collection(this.exchangeInfoBase);
  var candle =  candleStickSizeMinutes+'min';
 
  var set = {};
  set[candle] = csObject[candle];

  csCollection.update({period: csObject.period}, { $set : set },
    function(err, doc) {

      csDatastore.close();

      if(err) {

        callback(err);

      } else {
        //console.log('\n\n\nsuccess\n\n\n');
        callback(null);

      }
    }
  );

};

storage.prototype.pushIndicator = function(csObject, indicator, callback) {
  this.logger.log('\nStorage | pushIndicator\nPushing to '+this.exchangeInfoBase);
  
  var csDatastore = mongo(this.mongoConnectionString);
  var csCollection = csDatastore.collection(this.exchangeInfoBase);

  csCollection.insert(csObject, function(err, doc) {

    csDatastore.close();

    if(err) {

      callback(err);

    } else {

      callback(null);

    }

  });

};

storage.prototype.getLastTick = function(N, callback) {

  var csDatastore = mongo(this.mongoConnectionString);
  var csCollection = csDatastore.collection(this.exchangeTicks);

  csCollection.find({}).sort({date:-1}).limit(N, function(err, ticks) {

    csDatastore.close();

    if(err) {

      callback(err, []);

    } else {

      callback(null, ticks.reverse());

    }

  });


};

storage.prototype.getLastNTicks = function(N, callback) {

  var csDatastore = mongo(this.mongoConnectionString);
  var csCollection = csDatastore.collection(this.exchangeTicks);

  csCollection.find({}).sort({date:-1}).limit(N, function(err, ticks) {

    csDatastore.close();

    if(err) {

      callback(err, []);

    } else {

      callback(null, ticks.reverse());

    }

  });


};

storage.prototype.getLastNCandles = function(candleStickSizeMinutes, N, callback) {

  var candleMin = candleStickSizeMinutes+'min';
  console.log('\nStorage | getLastNCandles\nCandle Size: '+candleStickSizeMinutes+' | N: '+N);

  var csDatastore = mongo(this.mongoConnectionString);
  var csCollection = csDatastore.collection(this.exchangeInfoBase);

  var query = {};
  query[candleMin] = { $exists: true };

  csCollection.find(query).sort({period:-1}).limit(N, function(err, candlesSticks) {

    csDatastore.close();

    if(err) {

      callback(err, []);

    } else {

      callback(null, candlesSticks.reverse());

    }

  });


};

storage.prototype.getAllCandles = function(candleStickSizeMinutes, callback) {

  var candleMin = candleStickSizeMinutes+'min';
  var csDatastore = mongo(this.mongoConnectionString);
  var csCollection = csDatastore.collection(this.exchangeInfoBase);

  var query = {};
  query[candleMin] = { $exists: true };

  csCollection.find(query).sort({period:1}, function(err, candlesSticks) {

    csDatastore.close();

    if(err) {

      callback(err, []);

    } else {

      callback(null, candlesSticks);

    }

  });


};

storage.prototype.getAllCandlesSince = function(candleStickSizeMinutes, period, callback) {

  var candleMin = candleStickSizeMinutes+'min';
  var csDatastore = mongo(this.mongoConnectionString);
  var csCollection = csDatastore.collection(this.exchangeInfoBase);
  var query = {};
  query['period'] = { $gte: period };
  query[candleMin] = { $exists: true };

  csCollection.find(query).sort({period:1}, function(err, candlesSticks) {
    csDatastore.close();

    if(err) {

      callback(err, []);

    } else {

      callback(null, candlesSticks);

    }

  });


};

storage.prototype.getLastClose = function(candleStickSizeMinutes, callback) {

  var candleMin = candleStickSizeMinutes+'min';
  var csDatastore = mongo(this.mongoConnectionString);
  var csCollection = csDatastore.collection(this.exchangeInfoBase);

  var query = {};
  query[candleMin] = { $exists: true };

  csCollection.find(query).sort({period:-1}).limit(1, function(err, candleSticks) {

    csDatastore.close();

    if(err) {

      callback(err, 0);

    } else {

      if(candleSticks.length > 0) {
        callback(null, candleSticks[0].close);
      } else {
        callback(null, 0);
      }

    }

  });

};

storage.prototype.getLastNonEmptyPeriod = function(candleStickSizeMinutes, callback) {

  var candleMin = candleStickSizeMinutes+'min';
  var candleVol = candleMin+'.volume';
  var csDatastore = mongo(this.mongoConnectionString);
  var csCollection = csDatastore.collection(this.exchangeInfoBase);
  var query = {};
  query[candleMin] = { $exists: true };
  query[candleVol] = { $gt: 0 };

  csCollection.find(query).sort({period:-1}).limit(1, function(err, candleSticks) {

    csDatastore.close();

    if(err) {

      callback(err, 0);

    } else {

      if(candleSticks.length > 0) {
        callback(null, candleSticks[0].period);
      } else {
        callback(null, 0);
      }

    }

  });

};

storage.prototype.getLastNonEmptyClose = function(candleStickSizeMinutes, callback) {

  var candleMin = candleStickSizeMinutes+'min';
  var candleVol = candleMin+'.volume';
  var csDatastore = mongo(this.mongoConnectionString);
  var csCollection = csDatastore.collection(this.exchangeInfoBase);

  var query = {};
  query[candleMin] = { $exists: true };
  query[candleVol] = { $gte:0 };

  //console.log(JSON.stringify(query));

  csCollection.find(query).sort({period:-1}).limit(1, function(err, candleStick) {
    //console.log(JSON.stringify(candleStick));

    csDatastore.close();

    if(err) {

      callback(err, 0);

    } else {

      if(candleStick) {
        callback(null, candleStick);
      } else {
        callback(null, 0);
      }

    }

  });

};

storage.prototype.getLastNCompleteAggregatedCandleSticks = function(N, candleStickSizeMinutes, callback) {
  var candleMin = candleStickSizeMinutes+'min';
  console.log('\nStorage | getLastNCompleteAggregatedCandleSticks callback\nsize: '+candleStickSizeMinutes);

  this.getLastNAggregatedCandleSticks(N + 1, candleStickSizeMinutes, function(err, candleStickSizeMinutes, aggregatedCandleSticks) {
    console.log('\nStorage | getLastNAggregatedCandleSticks callback\nsize: '+candleStickSizeMinutes);
    aggregatedCandleSticks.pop();
    callback(null, candleStickSizeMinutes, aggregatedCandleSticks);
  }.bind(this));

};

storage.prototype.getCompleteAggregatedCandleSticks = function(candleStickSizeMinutes, callback) {

  this.getAggregatedCandleSticks(candleStickSizeMinutes, function(err, aggregatedCandleSticks) {
    aggregatedCandleSticks.pop();
    callback(null, aggregatedCandleSticks);
  });

};

storage.prototype.getLastCompleteAggregatedCandleStick = function(candleStickSizeMinutes, callback) {

  this.getLastNAggregatedCandleSticks(2, candleStickSizeMinutes, function(err, candleStickSizeMinutes, aggregatedCandleSticks) {
    //this.logger.log('**** Callback: getLastNAggregatedCandleSticks, size: '+candleStickSizeMinutes);
    //this.logger.log('**** Callback: aggregatedCandleSticks: '+JSON.stringify(aggregatedCandleSticks));
    aggregatedCandleSticks.pop();
    callback(null, candleStickSizeMinutes, _.last(aggregatedCandleSticks));
  }.bind(this));

};

storage.prototype.getLastNAggregatedCandleSticks = function(N, candleStickSizeMinutes, callback) {
  var candleMin = candleStickSizeMinutes+'min';
  this.logger.log('** getLastNAggregatedCandleSticks, size: '+candleStickSizeMinutes);

  var candleStickSizeSeconds = candleStickSizeMinutes * 60;

  var now = tools.unixTimeStamp(new Date().getTime());
  var closestCandleStick = (Math.floor(now/candleStickSizeSeconds)*candleStickSizeSeconds);

  var startRange = closestCandleStick - (candleStickSizeSeconds * N);

  this.getAllCandlesSince('1', startRange, function(err, candleSticks) {
    this.logger.log('***** CALLBACK: getAllCandlesSince, candleSticks length: '+candleSticks.length)

    if(candleSticks.length > 0) {

      var aggregatedCandleSticks = this.aggregateCandleSticks(candleStickSizeMinutes, candleSticks);

      callback(null, candleStickSizeMinutes, aggregatedCandleSticks);

    } else {

      callback(null, candleStickSizeMinutes, []);

    }

  }.bind(this));

};

storage.prototype.getAggregatedCandleSticks = function(candleStickSizeMinutes, callback) {

  this.getAllCandlesSince('1', 0, function(err, candleSticks) {

    if(candleSticks.length > 0) {

      var aggregatedCandleSticks = this.aggregateCandleSticks(candleStickSizeMinutes, candleSticks);

      callback(null, aggregatedCandleSticks);

    } else {

      callback(null, []);

    }

  }.bind(this));

};

storage.prototype.getAggregatedCandleSticksSince = function(candleStickSizeMinutes, period, callback) {

  this.getAllCandlesSince('1', period, function(err, candleSticks) {

    if(candleSticks.length > 0) {

      var aggregatedCandleSticks = this.aggregateCandleSticks(candleStickSizeMinutes, candleSticks);

      callback(null, aggregatedCandleSticks);

    } else {

      callback(null, []);

    }

  }.bind(this));

};

storage.prototype.calculateAggregatedCandleStick = function(candleStickSizeMinutes, period, relevantSticks) {

  //var currentCandleStick = {'period':period,'open':undefined,'high':undefined,'low':undefined,'close':undefined,'volume':0, 'vwap':undefined};
  var currentCandleStick = {};
  var candleMinute = candleStickSizeMinutes+'min';
  var candleInfo = {'open':undefined,'high':undefined,'low':undefined,'close':undefined,'volume':0,'vwap':undefined, 'macd': undefined, 'macdSignal': undefined, 'macdHistogram': undefined};
  
  //console.log('******** relevantSticks[0]['+candleMinute+']: '+JSON.stringify(relevantSticks[0]['1min']));

  candleInfo.open = relevantSticks[0]['1min'].open;
  candleInfo.high = _.max(relevantSticks, function(relevantStick) { return relevantStick['1min'].high; })['1min'].high;
  candleInfo.low = _.min(relevantSticks, function(relevantStick) { return relevantStick['1min'].low; })['1min'].low;
  candleInfo.close = relevantSticks[relevantSticks.length - 1]['1min'].close;
  candleInfo.volume = tools.round(_.reduce(relevantSticks, function(memo, entry) { return memo + entry['1min'].volume; }, 0), 8);
  if(candleInfo.volume === 0) {
    candleInfo.vwap = candleInfo.close;
  } else {
    candleInfo.vwap = tools.round(_.reduce(relevantSticks, function(memo, entry) { return memo + (entry['1min'].vwap * entry['1min'].volume); }, 0) / candleInfo.volume, 8);
  }

  currentCandleStick['period'] = period;
  currentCandleStick[candleMinute] = candleInfo;

  //console.log('******** aggregated candleStick: '+JSON.stringify(currentCandleStick));

  return currentCandleStick;

};

storage.prototype.aggregateCandleSticks = function(candleStickSize, candleSticks) {

  var candleStickSizeSeconds = 60 * candleStickSize;

  var aggregatedCandleSticks = [];

  var startTimeStamp = Math.floor(candleSticks[0].period / candleStickSizeSeconds) * candleStickSizeSeconds;
  var beginPeriod = startTimeStamp;
  var endPeriod = startTimeStamp + candleStickSizeSeconds;
  var stopTimeStamp = _.last(candleSticks).period;

  console.log('\ncandleSticks[0].period: '+candleSticks[0].period+' | beginPeriod: '+beginPeriod+' | endPeriod: '+endPeriod);

  var relevantSticks = [];

  var filterOnVolume = function(candleStick) { return candleStick['1min'].volume > 0; };

  _.each(candleSticks, function(candleStick) {
    //console.log('******* candleStick: '+JSON.stringify(candleStick));
    if(candleStick.period >= beginPeriod && candleStick.period < endPeriod) {

      relevantSticks.push(candleStick);

    } else {

      var vrelevantSticks = _.filter(relevantSticks, filterOnVolume);

      if(vrelevantSticks.length > 0) {
        relevantSticks = vrelevantSticks;
      }

      aggregatedCandleSticks.push(this.calculateAggregatedCandleStick(candleStickSize, beginPeriod, relevantSticks));

      beginPeriod = endPeriod;
      endPeriod = endPeriod + candleStickSizeSeconds;

      relevantSticks = [];

      relevantSticks.push(candleStick);

    }

  }.bind(this));

  if(relevantSticks.length > 0) {
    aggregatedCandleSticks.push(this.calculateAggregatedCandleStick(candleStickSize, beginPeriod, relevantSticks));
  }

  return aggregatedCandleSticks;

};

storage.prototype.aggregateCandleSticks2 = function(candleStickSize, candleSticks) {
    console.log('\ncandleSticks.length: '+candleSticks.length);

  // find this required candle's best divisor based on previous candle sizes, pCandleSize, from the config settings
  // - e.i if candleStickSize == 5, pCandleSize == 1, candleStickSize == 15, pCandleSize = 5, candleStickSize == 60, pCandleSize = 30
  // see if we can use this pCandleSize to save time calculating so we're not stuck using 1min candles for 12hr periods

  var index = this.candleStickSizeMinutesArray.indexOf(candleStickSize),
      pCandleSize = (index == 0 ? 1 : this.candleStickSizeMinutesArray[ index-1 ]),

      //check to see if we can use it to aggregate e.i no remainders (currently defaulting to 1 if not HCD)
      //TODO: update to actaully find highest common denominator (HCD).
      numToLoop = (candleStickSize % pCandleSize == 0 ? candleStickSize / pCandleSize : '1'),
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
  console.log('numToLoop: '+numToLoop+' | pCandleSize: '+pCandleSize+' | pCandleSizeString: '+pCandleSizeString);

  _.each(candleSticks, function(candleStick) {

    console.log('i: '+i+' | candleStick: '+JSON.stringify(candleStick));

    if( candleStick.period >= beginTimeStamp ){
      relevantSticks.push(candleStick);

      if( i % numToLoop == 0){        
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

storage.prototype.removeOldDBCandles = function(candleStickSizeMinutes, callback) {

  var candleStickSizeSeconds = candleStickSizeMinutes * 60;

  var csDatastore = mongo(this.mongoConnectionString);
  var csCollection = csDatastore.collection(this.exchangeInfoBase);

  var now = Math.floor(tools.unixTimeStamp(new Date().getTime()) / candleStickSizeSeconds) * candleStickSizeSeconds;
  var oldPeriod = now - (candleStickSizeSeconds * 10000);


  //console.log('\n\n\n\n ******* removing old candles, oldPeriod: '+oldPeriod);

  csCollection.remove({ period: { $lt: oldPeriod } }, function(err, resp) {

    csDatastore.close();

    callback(null);

  });

};

storage.prototype.dropCollection = function(collection, callback) {

  var csDatastore = mongo(this.mongoConnectionString);
  var csCollection = csDatastore.collection(collection);

  csCollection.drop(function(err) {

    csDatastore.close();

    callback(err);

  });

};

storage.prototype.setInitialBalance = function(initialBalance, callback) {

  var csDatastore = mongo(this.mongoConnectionString);
  var csCollection = csDatastore.collection('balance');

  csCollection.update({exchangePair: this.exchangePair}, {exchangePair: this.exchangePair, initialBalance: initialBalance}, { upsert: true }, function(err, doc) {

    csDatastore.close();

    if(err) {

      callback(err);

    } else {

      callback(null);

    }

  }.bind(this));

};

storage.prototype.getInitialBalance = function(callback) {

  var csDatastore = mongo(this.mongoConnectionString);
  var csCollection = csDatastore.collection('balance');

  csCollection.find({exchangePair: this.exchangePair}).limit(1, function(err, doc) {

    csDatastore.close();

    if(err) {

      callback(err);

    } else if(doc.length > 0 ){

      var initialBalance = doc[0].initialBalance;

      callback(null, initialBalance);

    } else {

      callback(null, null);

    }

  }.bind(this));

};

module.exports = storage;
