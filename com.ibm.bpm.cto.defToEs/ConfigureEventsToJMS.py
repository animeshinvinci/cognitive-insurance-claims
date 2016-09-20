#BEGIN COPYRIGHT
#*************************************************************************
#
# Licensed Materials - Property of IBM
# 5725-C94, 5725-C95, 5725-C96
# (C) Copyright IBM Corporation 2014. All Rights Reserved.
# US Government Users Restricted Rights- Use, duplication or disclosure
# restricted by GSA ADP Schedule Contract with IBM Corp.
#
#*************************************************************************
#END COPYRIGHT

# A unique id for the listener configuration to be created.
# This is a simple string that will be used to identify 
# this DEF configuration.
defListenerId = 'elkJmsListener'

# The JNDI name of the target JMS queue.  This is the JNDI name
# of an existing queue that has already been created on your
# deployment manager console.

eventQueueJndiName = 'jms/myDefQ'

# The JNDI name of a connection factory appropriate for the target JMS queue
# identified by the eventQueueJndiName setting.  This is the JNDI name
# of an existing queue that has already been created on your
# deployment manager console.  The queue connection factory should be
# created and associated with BPM Server's bus.  It should also specify
# the authorization alias which should match the value specified by 
# the eventQueueCF_AuthorizationAlias parameter.

eventQueueCFJndiName = 'jms/myDefQCF'

# Specify the Authentication Alias to use.  This alias should  
# be defined first on the deployment manager.  In this sample, we are
# utilizing the deployment environment authorization alias that already
# exists.
eventQueueCF_AuthorizationAlias = 'DeAdminAlias'


# A list of event point keys that you wish to register interest in
# receiving.  The '*' is a wildcard character identifying which events will be emitted AND
# listened to. For additional information see: 
# http://www.ibm.com/support/knowledgecenter/SSFPJS_8.5.5/com.ibm.wbpm.mon.imuc.doc/intro/intro_event_point_key.html
#
# The subscriptions represent an array of 7 part keys that are separated by
# the '/' character.
#
# The positions match the table that appears in the Information Center topic noted above.  As an
# example, the first key represents the Application Name (the acryonym field for a BPD),
# the second key represents the version, the third key represents the component type, etc, etc.
#
# If you wish to simply listen to everything, you would use a subscription setting of '/*/*/*/*/*/*'
#
subscriptions = [ 
 '*/*/*/*/*/*/*'
]

#########################################################
# Do not change anything beyond this line in the script.
#########################################################


defListenerFactoryId = 'com.ibm.bpm.def.JmsDefEventListenerFactory'

# find the cell's def listener configs
defListenerConfig = AdminConfig.getid('/Cell:/DefListenerConfig:/')

# if the cell's def listner config was not found, create it
if not defListenerConfig:
    defListenerConfig = AdminConfig.create('DefListenerConfig', AdminConfig.getid('/Cell:/'), [])

# get the jms def listener
defListener = None
for possibleDefListener in AdminConfig.getid('/Cell:/DefListenerConfig:/DefListener:/').splitlines() :
    if AdminConfig.showAttribute(possibleDefListener, 'listenerId') == defListenerId:
        defListener = possibleDefListener
        break

# throw exception if the def listener already exists
if defListener:
    raise 'A DefListener with an id of %s already exists.' % defListenerId
    
# create the jms def listener
defListenerAttributes = []
defListenerAttributes.append(['listenerId', defListenerId])
defListenerAttributes.append(['listenerFactoryId', defListenerFactoryId])
defListener = AdminConfig.create('DefListener', defListenerConfig, defListenerAttributes)

# specify the jms queue to use
AdminConfig.create('DefProperty', defListener, [['name', 'JMS_QUEUE_JNDI'], ['value', eventQueueJndiName]])

# specify the jms queue connection factory to use
AdminConfig.create('DefProperty', defListener, [['name', 'JMS_QUEUE_CF_JNDI'], ['value', eventQueueCFJndiName]])

# specify the Authorization Alias to be used by the jms queue connection factory 
AdminConfig.create('DefProperty', defListener, [['name', 'JMS_AUTHENTICATION_ALIAS'], ['value', eventQueueCF_AuthorizationAlias]])





# specify the event to listen to using a filter
index = -1
for subscription in subscriptions:
    index += 1
    filterFields = subscription.split('/')
    if len(filterFields) != 7:
        raise 'Error parsing subscription string. Expecting 7 fields, found %s' % len(filterFields)
    filterAttributes = []
    filterAttributes.append(['appName', filterFields[0]])
    filterAttributes.append(['version', filterFields[1]])
    filterAttributes.append(['componentType', filterFields[2]])
    filterAttributes.append(['componentName', filterFields[3]])
    filterAttributes.append(['elementType', filterFields[4]])
    filterAttributes.append(['elementName', filterFields[5]])
    filterAttributes.append(['nature', filterFields[6]])
    filterAttributes.append(['filterId', '%s_%s' % (defListenerId,index)])
    filter = AdminConfig.create('DefFilter', defListener, filterAttributes)

# find the def producer config
defProducerConfig = AdminConfig.getid('/Cell:/DefProducerConfig:/')

# create new def producer config if needed
if not defProducerConfig:
    defProducerConfig = AdminConfig.create('DefProducerConfig', AdminConfig.getid('/Cell:/'), [])

# check for exising def producer
defProducerId = 'ProducerFor_%s' % defListenerId
defProducer = None
for possibleDefProducer in AdminConfig.getid('/Cell:/DefProducerConfig:/DefProducer:/').splitlines():
    if AdminConfig.showAttribute(possibleDefProducer,'producerId') == defProducerId:
        defProducer = possibleDefProducer

if defProducer:
    raise 'A DefProducer already exists with an id of %s.' % defProducerId

# create the def producer
defProducer = AdminConfig.create('DefProducer', defProducerConfig, [['producerId', defProducerId]])

# specify the events to produce using a filter
index = -1
for subscription in subscriptions:
    index += 1
    filterFields = subscription.split('/')
    if len(filterFields) != 7:
        raise 'Error parsing subscription string. Expecting 7 fields, found %s' % len(filterFields)
    filterAttributes = []
    filterAttributes.append(['appName', filterFields[0]])
    filterAttributes.append(['version', filterFields[1]])
    filterAttributes.append(['componentType', filterFields[2]])
    filterAttributes.append(['componentName', filterFields[3]])
    filterAttributes.append(['elementType', filterFields[4]])
    filterAttributes.append(['elementName', filterFields[5]])
    filterAttributes.append(['nature', filterFields[6]])
    filterAttributes.append(['filterId', '%s_%s' % (defProducerId,index)])
    filter = AdminConfig.create('DefFilter', defProducer, filterAttributes)

AdminConfig.save()
