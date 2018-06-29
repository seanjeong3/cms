var mosca = require('mosca')
var cassandra = require('cassandra-driver');

var cassIP = ['0.0.0.0:9042'];
var cassKeyspace = 'cms_system';
const authProvider = new cassandra.auth.PlainTextAuthProvider(process.argv[2], process.argv[3]);
const cassClient = new cassandra.Client({ contactPoints: cassIP, keyspace: cassKeyspace, authProvider: authProvider});



var settings = {
  port: 1883
};
 
//here we start mosca
var server = new mosca.Server(settings);
server.on('ready', setup);
 
// fired when the mqtt server is ready
function setup() {
  console.log('Mosca server is up and running')
}
 
// fired whena  client is connected
server.on('clientConnected', function(client) {
  console.log('client connected', client.id);
});
 
// fired when a message is received
server.on('published', function(packet, client) {
  //console.log('Published : ', packet.payload);
  if (packet.topic == 'machine/gateway/out/preprocessed_data') {
        store_preprocessed_data(packet.payload.toString());
  } else if (packet.topic == 'machine/gateway/out/raw_data_abnormal') {
        store_raw_data_abnormal(packet.payload.toString());
  } else if (packet.topic == 'machine/gateway/out/raw_data_regular') {
        store_raw_data_regular(packet.payload.toString());
  };

});

function store_preprocessed_data(data) {
  console.log(data);
  var body = JSON.parse(data);
  const query = 'INSERT INTO sensor_data_preprocessed (sensor_id, year, event_time, min, max) values (?, ?, ?, ?, ?)';
  var queries = []
  for (var i=0; i<body.length; i++){
    var ts = new Date(Date.parse(body[i].event_time));
    console.log(body[i].event_time)
    var params = [body[i].sensor_id, (dateFormat(ts, "yyyy")), ts, body[i].data];
    queries.push({'query':query, 'params':params});
  }
  cassClient.batch(queries, { prepare: true }, function (err) {
      if (err) {
        console.log(err);
       }
    return;
  });
}


function store_raw_data_abnormal(data) {
  console.log(data);
  var body = JSON.parse(data);
  const query = 'INSERT INTO sensor_data_raw_abnormal (sensor_id, year, event_time, data) values (?, ?, ?, ?)';
  var queries = []
  for (var i=0; i<body.length; i++){
    var ts = new Date(Date.parse(body[i].event_time));
    console.log(body[i].event_time)
    var params = [body[i].sensor_id, (dateFormat(ts, "yyyy")), ts, body[i].data];
    queries.push({'query':query, 'params':params});
  }
  cassClient.batch(queries, { prepare: true }, function (err) {
      if (err) {
        console.log(err);
       }
    return;
  });
}

function store_raw_data_regular(data) {
  console.log(data);
  var body = JSON.parse(data);
  const query = 'INSERT INTO sensor_data_raw_regular (sensor_id, year, event_time, data) values (?, ?, ?, ?)';
  var queries = []
  for (var i=0; i<body.length; i++){
    var ts = new Date(Date.parse(body[i].event_time));
    console.log(body[i].event_time)
    var params = [body[i].sensor_id, (dateFormat(ts, "yyyy")), ts, body[i].data];
    queries.push({'query':query, 'params':params});
  }
  cassClient.batch(queries, { prepare: true }, function (err) {
      if (err) {
        console.log(err);
       }
    return;
  });
}
 
// fired when a client subscribes to a topic
server.on('subscribed', function(topic, client) {
  console.log('subscribed : ', topic);
});
 
// fired when a client subscribes to a topic
server.on('unsubscribed', function(topic, client) {
  console.log('unsubscribed : ', topic);
});
 
// fired when a client is disconnecting
server.on('clientDisconnecting', function(client) {
  console.log('clientDisconnecting : ', client.id);
});
 
// fired when a client is disconnected
server.on('clientDisconnected', function(client) {
  console.log('clientDisconnected : ', client.id);
});

