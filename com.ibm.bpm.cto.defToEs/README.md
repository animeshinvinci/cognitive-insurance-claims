This project implements a JMS Listener for IBM BPM's Dynamic Event Framework (DEF).  To build this ear run `gradle war` from this directory.  Please note that you need to run the following against your BPM instance:

1. create the queue `jms/myDefQ`
1. create the queue connection factory `jms/myDefQCF`
1. create the activiation spec `jms/myDefAS`
1. run the scripts in [ConfigureEventsToJMS.py](../bpm_twx/ConfigureEventsToJMS.py]
1. create a JAAS entry (via the admin console) with the following data:
  `KafkaClient {
      com.ibm.messagehub.login.MessageHubLoginModule required
      serviceName="kafka"
      username="2Y2hYOYDO5UmpWMC"
      password="hhZlRLa6CCLJe9O6wLt5x2HzTdGGkesi";
  }; `
