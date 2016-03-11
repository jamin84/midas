var _ = require('underscore');
var mongo = require('mongojs');
var async = require('async');
var tools = require('../util/tools.js');

var storage = function(exchangeSettings, mongoConnectionString, logger) {

  this.pair = exchangeSettings.currencyPair.pair;
  this.exchange = exchangeSettings.exchange;
  this.exchangeBase = exchangeSettings.exchange + exchangeSettings.currencyPair.pair;
  this.exchangeInfoBase = this.exchangeBase + '_Info';
  this.exchangeTicks = exchangeSettings.exchange+ exchangeSettings.currencyPair.pair + '_Ticks';
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

      callback(err);

    } else {

      callback(null);

    }

  });

};

storage.prototype.push = function(candleStickSizeMinutes, csObject, callback) {
  this.logger.log('* Pushing to '+this.exchangeInfoBase+' at period: '+csObject.period+' for '+candleStickSizeMinutes+'min candle');

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
        console.log('\n\n\nsuccess\n\n\n');
        callback(null);

      }
    }
  );

};

storage.prototype.pushIndicator = function(csObject, indicator, callback) {
  this.logger.log('* Pushing to '+this.exchangeInfoBase);
  
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
  this.logger.log('* getLastNCandles, size: '+candleStickSizeMinutes+' | N: '+N);

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
  var candleVol = candleMin+'volume';
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
  var candleVol = candleMin+'volume';
  var csDatastore = mongo(this.mongoConnectionString);
  var csCollection = csDatastore.collection(this.exchangeInfoBase);

  var query = {};
  query[candleMin] = { $exists: true };
  query[candleVol] = { $gte:0 };

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

storage.prototype.getLastNCompleteAggregatedCandleSticks = function(N, candleStickSizeMinutes, callback) {
  var candleMin = candleStickSizeMinutes+'min';
  this.logger.log('* getLastNCompleteAggregatedCandleSticks callback, size: '+candleStickSizeMinutes);

  this.getLastNAggregatedCandleSticks(N + 1, candleStickSizeMinutes, function(err, candleStickSizeMinutes, aggregatedCandleSticks) {
    this.logger.log('*** getLastNAggregatedCandleSticks callback, size: '+candleStickSizeMinutes);
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
  
  console.log('******** relevantSticks[0]['+candleMinute+']: '+JSON.stringify(relevantSticks[0]['1min']));

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

  console.log('******** aggregated candleStick: '+JSON.stringify(currentCandleStick));

  return currentCandleStick;

};

storage.prototype.aggregateCandleSticks = function(candleStickSize, candleSticks) {

  var candleStickSizeSeconds = 60 * candleStickSize;

  var aggregatedCandleSticks = [];

  var startTimeStamp = Math.floor(candleSticks[0].period / candleStickSizeSeconds) * candleStickSizeSeconds;
  var beginPeriod = startTimeStamp;
  var endPeriod = startTimeStamp + candleStickSizeSeconds;
  var stopTimeStamp = _.last(candleSticks).period;

  var relevantSticks = [];

  var filterOnVolume = function(candleStick) { return candleStick['1min'].volume > 0; };

  _.each(candleSticks, function(candleStick) {
    console.log('******* candleStick: '+JSON.stringify(candleStick));
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

storage.prototype.removeOldDBCandles = function(candleStickSizeMinutes, callback) {

  var candleStickSizeSeconds = candleStickSizeMinutes * 60;

  var csDatastore = mongo(this.mongoConnectionString);
  var csCollection = csDatastore.collection(this.exchangeInfoBase);

  var now = Math.floor(tools.unixTimeStamp(new Date().getTime()) / candleStickSizeSeconds) * candleStickSizeSeconds;
  var oldPeriod = now - (candleStickSizeSeconds * 10000);


  console.log('\n\n\n\n ******* removing old candles, oldPeriod: '+oldPeriod);

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
