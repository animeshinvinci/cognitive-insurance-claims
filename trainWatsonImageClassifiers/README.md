# bpmNext Test Clients
This project contains two Scala classes which:

1. Kicks off a load test of 500 BPD instances in IBM BPM on Cloud
1. Queries the status of those processes.

You will need `sbt` to compile and run these classes.

To kick off the test run you execute `sbt "run-main com.ibm.cicto.TrainWatsonImage"`.  You will see lots of output as BPDs are created via the REST API.  
