# odbc-watch

Watch to result of multiple SQL queries in parallel using MS SQL Server Query Notifications.

## example

```
var db = require("odbc")();
var watch = require("odbc-watch");

var cn = "DRIVER={SQL Server Native Client 11.0};SERVER=127.0.0.1;DATABASE=MyDatabase;UID=MyUser;PWD=MyPassword;"

var watchArgs = {
  queue: "MyChangeMessages",
  options: "service=MyChangeNotifications",
  timeout: 120,
  fetchResults: true,
  subscriptions: {
    "sessions": "SELECT sessionId, sessionData, lastTouchedUtc FROM dbo.Session WHERE sessionId IS NOT NULL"
  }
};

db.open(cn, function (err) {
  if (err) return util.log(err);

    var ee = watch(db, watchArgs);

    ee.on('error', function (err) {
        throw err;
    });

    ee.on('timeout', function (subscription) {
        console.log("timeout on subscription", subscription);
    });

    ee.on('change', function(subscription, data, details) {
        console.log("subscription", subscription, "changed!");
        console.log("details:", details);
        console.log("data:", data);
    });
});
```