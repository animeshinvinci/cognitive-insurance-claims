# Claim Predictions using spark-jobserver
This project creates a predictive model based upon all completed claims information in IBM Object Storage in Bluemix.

It also includes:
1. a [Spark job](src/main/scala/com/ibm/bpm/cloud/ci/cto/prediction/SparkModelJob.scala) which generates a predictive model based upon data in object storage
1. a [predictive Spark job](src/main/scala/com/ibm/bpm/cloud/ci/cto/prediction/PredictLoanJob.scala) which takes a given claim's info, uses the model previously generated, and predicts whether to approve or reject the claim.

To build this project run ```sbt assembly``` and then run the following code snippets:
1.  Upload the generated jar file to spark-jobserver:  ```curl --data-binary @target/scala-2.10/SparkJobServer-assembly-1.0.jar 169.44.9.196:8090/jars/test```
2.  Create the context where the Spark jobs should execute: ```curl -d "" '169.44.9.196:8090/contexts/model-context'```
3.  Create the predictive model:  ```curl -d "" '169.44.9.196:8090/jobs?appName=test&classPath=com.ibm.bpm.cloud.ci.cto.prediction.SparkModelJob&context=model-context'```
4.  Verify the model was created successfuly by asking for a prediction:  ```curl -d "input.approvedAmount=1000, input.estimate=2000, input.creditScore=850" '169.44.9.196:8090/jobs?appName=test&classPath=com.ibm.bpm.cloud.ci.cto.prediction.PredictLoanJob&context=model-context&sync=true'```

If you need to start over you can delete the context by running ```curl -X DELETE '169.44.9.196:8090/contexts/model-context'```
