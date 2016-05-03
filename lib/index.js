var assert = require('assert');
var events = require('events');

var async = require("async");
var uuid = require("uuid");
var xml2js = require("xml2js");
var _ = require("lodash");

module.exports = watch;

/**
 * returns EventEmitter that emits the following events:
 *   'error'   - in case an error happens. watch will abort on error
 *   'change'  - in case the result of one of the provided queries changes
 *   'timeout' - a timeout ocurred on one or more subscriptions
 */
function watch(db, args) {
    var ee = new events.EventEmitter();
    
    assert(args.queue);
    assert(args.options);
    assert(args.subscriptions);

    // fetch results or just emit change events
    var fetchResults = !!args.fetchResults;
    var queue = args.queue;

    var subscriptionId = uuid.v4();
    var subscriptions = parseSubscriptionArgs(args, subscriptionId);
    if (Object.keys(subscriptions).length === 0) {
        throw new Error("empty subscrptions provided");
    }

    // allow calling scope to register on all events before doing anything
    setImmediate(startWatchLoop);

    return ee;

    function startWatchLoop() {
        clearQueue(function(err) {
            if (err) { return ee.emit('error', err); }

            subscribeAll(function(err) {
                if (err) { return ee.emit('error', err); }

                return watchLoop();
            });
        });
    }

    // TODO: check whether queue contains elements we might be interested in!
    function clearQueue(cb) {
        var blocking = false;
        checkSubscriptionImpl(blocking, function (err, result) {
            if (err) return cb(err);

            // queue is empty, we can stop clearing now.
            if (!result) {
                return cb(null);
            }

            return clearQueue(cb);
        });
    }

    function subscribeAll(cb) {
        async.eachSeries(_.values(subscriptions), subscribeImpl, cb);
    }

    function watchLoop() {
        watchImpl(function (err, result) {
            if (err) {
                ee.emit('error', err);
                return; // abort on error
            }

            if (result && result.subscription) {
                subscribeAgain(result.subscription);
            } else {
                watchLoop();
            }
        });
    }

    function subscribeAgain(subscription) {
        subscribeImpl(subscriptions[subscription], function(err) {
            if (err) return ee.emit('error', err);

            watchLoop();
        });
    }

    function watchImpl(cb) {
        var blocking = true;
        checkSubscriptionImpl(blocking, function (err, result) {
            if (err) { return cb(err); }

            // Ignoring subscription that was not triggered by us
            if (subscriptionId !== result.subscriptionId) {
                //console.log("Ignoring subscription meant for somebody else:", JSON.stringify(result));
                return cb(null);
            }

            if (result.source === 'timeout' && result.info === 'none') {
                ee.emit('timeout', result.subscription);
		// in case of timeout, all subscriptions were invalidated
                return subscribeAll(cb);
            }

            var VALID_DATA_INFO_TYPES = {
                'insert': true,
                'update': true,
                'delete': true,
            };
            if (result.source === 'data' && VALID_DATA_INFO_TYPES[result.info]) {
                var details = _.pick(result, 'source', 'info');
                if (!fetchResults) {
                    ee.emit('change', result.subscription, null, details);
                    return cb(null, { subscription: result.subscription });
                } else {
                    var sql = subscriptions[result.subscription].sql;
                    return db.query(sql, function (err, sqlResult) {
                        if (err) return cb(err);

                        ee.emit('change', result.subscription, sqlResult, details);
                        return cb(null, { subscription: result.subscription });
                    });
                }
            }

            if (result.source === 'statement' && result.info === 'invalid') {
                return cb(new Error('Unable to subscribe on invalid statement for ' +
                    result.subscription + ': ' + subscriptions[result.subscription].sql));
            }

            return cb(new Error('Unknown QueryNotification result: ' + JSON.stringify(result)));
        });
    }

    function subscribeImpl(args, cb) {
        clearQueue(function (err) {
            if (err) return cb(err);

            var subscribeArgs = _.omit(args, 'name');
            db.subscribe(subscribeArgs, function (err) {
                if (err) return cb(err);

                return cb(null);
            });
        });
    }

    function checkSubscriptionImpl(blocking, cb) {
        var sql = "RECEIVE * FROM " + queue;
        if (blocking) {
            sql = "WAITFOR (" + sql + ")";
        }

        db.query(sql, function (err, result) {
            if (err) { return cb(err); }

            if (!result) {
                return cb(null, null);
            }

            return parseSubscriptionResult(result, cb);
        });
    }
}

function parseSubscriptionResult(result, cb) {
    if (!Array.isArray(result) || result.length > 1) {
        return cb(new Error("invalid result"));
    }

    if (result.length < 1) {
        return cb(null, null);
    }

    var row = result[0];
    if (!row.message_body) {
        return cb(new Error("Result does not contain a message body."));
    }

    var xml = new Buffer(row.message_body, 'hex').toString('utf16le', 2);
    xml2js.parseString(xml, function (err, json) {
        if (err) { return cb(err); }

        var message;
        try {
            message = JSON.parse(json["qn:QueryNotification"]["qn:Message"][0]);
        } catch (ex) {
            return cb(ex);
        }

        return cb(null, {
            info: json["qn:QueryNotification"].$.info,
            source: json["qn:QueryNotification"].$.source,
            subscription: message.subscription,
            subscriptionId: message.id,
        });
    });
}

function parseSubscriptionArgs(args, subscriptionId) {
    var subscriptions = {};
    _.forEach(args.subscriptions, function (sql, name) {
        var baseArgs = _.pick(args, ["queue", "options", "timeout"]);
        // TODO: allow overwriting with custom message?
        var message = {subscription: name, id: subscriptionId};
        subscriptions[name] = _.assign(baseArgs, {
            message: JSON.stringify(message),
            name: name,
            sql: sql,
        });
    });
    return subscriptions;
}
