var express = require('express');  
var request = require('request');
var formidable = require("formidable");
var util = require('util');
var apiServerHost="https://gateway-a.watsonplatform.net";

var app = express();  

app.use('/', function(req, res) {  
console.log(req.params);
console.log(req.query);
console.log(req.body);
console.log(req.headers);
var form = new formidable.IncomingForm();
form.parse(req, function(err, fields, files){
	console.log(util.inspect({fields:fields,files:files}));
});

  var url = apiServerHost + req.url;
  req.pipe(request(url)).pipe(res);
});

app.listen(process.env.PORT || 3000);  

