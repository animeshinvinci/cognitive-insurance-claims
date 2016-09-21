var express = require('express');
var router = express.Router();
var unirest = require('unirest');
//var redis = require("redis"), client=redis.createClient("redis://x:DQJIQGKUYGBCNKTN@sl-us-dal-9-portal.1.dblayer.com:15146");

// client.on("error", function (err) {
//     console.log("Error " + err);
// });


/* GET home page. */
/* Caching example for Eric
router.post('/', function(req, res, next) {
  const creditScore=req.body.creditScore;
  const approvedAmount=req.body.approvedAmount;
  const estimateAmount=req.body.estimateAmount;
  const cachedValue=client.get("creditScore:"+creditScore+":approvedAmount:"+approvedAmount+":estimateAmount:"+estimateAmount);
  var prediction;
  if(cachedValue===null){
    unirest.post("http://169.44.9.196:8090/jobs?appName=test&classPath=com.ibm.bpm.cloud.ci.cto.prediction.PredictClaimJob&context=model-context&sync=true")
      .send("input.approvedAmount=" + approvedAmount + ", input.estimate=" + estimateAmount + ", input.creditScore=" + creditScore))
      .end(function(response){
        prediction = response.body.result;
        client.set("creditScore:"+creditScore+":approvedAmount:"+approvedAmount+":estimateAmount:"+estimateAmount, prediction);
        client.quit();
    });
  }else{
    prediction=cachedValue;
  }
  console.log(req.body);
  res.render('sendData', { title: 'Cognitive Claims', prediction: prediction});
});
*/

router.post("/", function (req, res, next){
  console.log(req.body);
  const amount=parseInt(req.body.amount);
  const estimate=parseInt(req.body.estimate);
  const creditScore=parseInt(req.body.creditScore);
    //Get current model
  var prediction;
  var request={};
  request.amount=amount;
  request.estimate=estimate;
  request.creditScore=creditScore;
  console.log("request is");
  console.log(request);
  unirest.post("http://169.44.9.196:8090/jobs?appName=test&classPath=com.ibm.bpm.cloud.ci.cto.prediction.PredictClaimJob&context=model-context&sync=true")
  .send("input.approvedAmount="+amount+", input.estimate="+estimate+", input.creditScore="+creditScore)
  .end(function(response){
    console.log("response is")
    console.log(response.body);
    prediction = response.body.result;
    console.log(request);
    res.render('predictionTestResult', { title: 'Cognitive Claims', request:request, prediction: prediction});
  });

});


module.exports = router;
