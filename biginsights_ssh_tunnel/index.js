var tunnel = require('tunnel-ssh');
var express = require('express');
var request = require('request');


var app = express();
var config = {
  dstHost:'localhost',
  host:'bi-hadoop-prod-4060.bi.services.us-south.bluemix.net',
  port:22,
  dstPort:8090,
  username:'hadoop',
  password: 'hs8muoNhWYJvKL4Ew',
  //keepalive: true,
  //debug: function(stuff){console.log(stuff)}
};


app.use('/', function(req, res) {
  //We need to get a random local port since each request tries to open a new
  //SSH tunnel on the same port which causes PortInUse errors.
  var port = getRandomPort();
  var conf=config;
  conf.localPort=port;

  //Open the tunnel
  var server = tunnel(conf, function(error,server){
    if(error){
      console.log(error)
    }
  });
  server.on('error', function(err){
    console.error(err);
  });

  //Redirect the current HTTP request to go through the SSH tunnel to BigInsights
  var url = "http://localhost:"+port+ req.url;
  console.log(url);
  //Pipe the response from BI back to the caller.
  req.pipe(request(url)).pipe(res);
});


function getRandomPort() {
    return Math.floor(Math.random() * (9000 - 5000 + 1)) + 5000;
}


app.listen(process.env.PORT || 3000);
