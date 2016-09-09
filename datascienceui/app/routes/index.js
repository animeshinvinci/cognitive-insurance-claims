var express = require('express');
var router = express.Router();
var unirest = require('unirest');

/* GET home page. */
router.get('/', function(req, res, next) {
unirest.post("http://169.44.9.196:8090/jobs?appName=test&classPath=com.ibm.bpm.cloud.ci.cto.prediction.GetModelJob&context=model-context&sync=true")
  .end(function(response){
    console.log(response.body);
    const model = response.body.result;
    console.log(model)
  res.render('index', { title: 'Cognitive Claims', model: model });
  });
});

module.exports = router;
