var express = require('express');
var router = express.Router();
var unirest = require('unirest');
var awty = require('awty');

/* GET home page. */
router.get('/', function(req, res, next) {
  //Get current model
  var oldModel;
  unirest.post("http://169.44.9.196:8090/jobs?appName=test&classPath=com.ibm.bpm.cloud.ci.cto.prediction.GetModelJob&context=model-context&sync=true")
  .end(function(response){
    //console.log(response.body);
    oldModel = response.body.result;
  });
  const newModelType = req.query.model;

  unirest.post("http://169.44.9.196:8090/jobs?appName=test&classPath=com.ibm.bpm.cloud.ci.cto.prediction.SparkModelJob&context=model-context")
  .send({"input.modelType":newModelType})
  .end(function(response){
    console.log(response.body);
    const jobId = response.body.result.jobId;
    var jobDone=false;
    var poll = awty(function(){
      unirest.get("http://169.44.9.196:8090/jobs/"+jobId)
      .end(function(response){
        if(response.body.status==="FINISHED"){
          //console.log("done polling")
          jobDone = true;
        }else{
          //console.log("still polling")
          jobDone = false;
        }
      });
      return jobDone;
    });
    poll.every(500);
    poll(function(fin){
      if(fin){
        console.log("model creation complete");
        unirest.get("http://169.44.9.196:8090/jobs/"+jobId)
        .end(function(response){
          var model=response.body.result;
          res.render('newModel', { title: 'Review Model', oldModel: oldModel, newModel: model });
        });
      }
    });
  });
});
module.exports = router;
