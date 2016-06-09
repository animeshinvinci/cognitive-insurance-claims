This project implements a JMS Listener for IBM BPM's Dynamic Event Framework (DEF).  To build this ear run `gradle war` from this directory.  Please note that you need to run the following against your BPM instance:

1. create the queue `jms/myDefQ`
1. create the queue connection factory `jms/myDefQCF`.  *Must* be a queue connection factory or you will get a `ClassCastException` from DEF and Karri will be sad. 😒
1. create the activiation spec `jms/myDefAS`
1. run the scripts in [ConfigureEventsToJMS.py](../bpm_twx/ConfigureEventsToJMS.py]
1. create a JAAS application login entry (via the admin console) with the following data, replacing the username and password with your provisioned Message Hub username and password:
```
KafkaClient {
      com.ibm.messagehub.login.MessageHubLoginModule required
      serviceName="kafka"
      username="$MESSAGE_HUB_USERNAME"
      password="$MESSAGE_HUB_PASSWORD";
  }; 
```
1.  install the generated ear file to the apptarget cluster
