# bpmNext Test Clients
This project contains two Scala classes which:

1. Kicks off a load test of 500 BPD instances in IBM BPM on Cloud
1. Queries the status of those processes.

You will need `sbt` to compile and run these classes.

To kick off the test run you execute `sbt "run-main com.ibm.cicto.CallCognitiveClaim"`.  You will see lots of output as BPDs are created via the REST API.  

To get the current status of those 500 BPDs you run `sbt "run-main com.ibm.cicto.GetProcessStatus"`.  You will see some debug output before you finally see some JSON representing your process instances:
```json
{"Completed":500,"Active":0,"Failed":0,"Terminated":0,"Did_not_Start":0,"Suspended":0}
```
