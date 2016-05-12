package com.ibm.bpm.cto;

import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import javax.annotation.PreDestroy;
import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;

/**
 * Message-Driven Bean implementation class for: DEFtoESBean
 */
@MessageDriven(activationConfig = {
		@ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
		@ActivationConfigProperty(propertyName = "destinationJndiName", propertyValue = "jms/myDefQ"), })
public class DEFtoKafkaMDB implements MessageListener {
	private static final String CLASS_NAME = DEFtoKafkaMDB.class.getName();
	public static final Logger LOG = Logger.getLogger(CLASS_NAME);

	// Should probably create a topic per MM
	private static final String TOPIC = "bpmNextMMTopic";
	private Map<String, Object> kafkaConnectionProps = new HashMap<>();
	KafkaProducer<String, String> kafkaProducer;

	/**
	 * Default constructor.
	 */
	public DEFtoKafkaMDB() {
		System.out.println("In init");
		if (kafkaProducer == null) {
			synchronized (String.class) {
				// These could be externalized into a properties file for more
				// flexibility.
				kafkaConnectionProps.put("bootstrap.servers",
						"kafka01-prod01.messagehub.services.us-south.bluemix.net:9093,kafka02-prod01.messagehub.services.us-south.bluemix.net:9093,kafka03-prod01.messagehub.services.us-south.bluemix.net:9093,kafka04-prod01.messagehub.services.us-south.bluemix.net:9093,kafka05-prod01.messagehub.services.us-south.bluemix.net:9093");
				kafkaConnectionProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
				kafkaConnectionProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
				kafkaConnectionProps.put("client.id", "deftokafka");
				kafkaConnectionProps.put("acks", "-1");
				kafkaConnectionProps.put("security.protocol", "SASL_SSL");
				kafkaConnectionProps.put("ssl.protocol", "TLSv1.2");
				kafkaConnectionProps.put("ssl.enabled.protocols", "TLSv1.2");
				kafkaConnectionProps.put("ssl.truststore.type", "JKS");
				kafkaConnectionProps.put("ssl.endpoint.identification.algorithm", "HTTPS");
				kafkaConnectionProps.put("ssl.truststore.location",
						System.getProperty("java.home") + "/lib/security/cacerts");
				kafkaConnectionProps.put("ssl.truststore.password", "changeit");
				kafkaProducer = new KafkaProducer<String, String>(kafkaConnectionProps);
				System.out.println("BU: connecting to kafka " + this.kafkaConnectionProps);
			}
		}
	}

	/**
	 * @see MessageListener#onMessage(Message)
	 */
	@TransactionAttribute(value = TransactionAttributeType.REQUIRED)
	public void onMessage(Message message) {
		TextMessage m = (TextMessage) message;
		try {
			// Remove the namespaces from the DEF event payload, it just makes
			// it harder to address the individual elements downstream
			String noNamespace = doXSLT(m.getText().trim());
			// Convert XML DEF payload string to a JSON Object
			JSONObject jsono = XML.toJSONObject(noNamespace);
			// Create a Kafka record out of this JSON object. In this case it is
			// the string representation of the DEF object.
			// Need to look at using a JSONObject serializer instead of string a
			// string to simplify things downstream
			ProducerRecord<String, String> kafkaMessage = new ProducerRecord<String, String>(TOPIC, jsono.toString());
			// System.out.println("BU producer record " + kafkaMessage);
			// Send the DEF event to Kafka
			this.kafkaProducer.send(kafkaMessage);
			// System.out.println("BU: message sent");
		} catch (JSONException | JMSException e) {
			System.out.println("failed due to " + e.getMessage());
			e.printStackTrace();
		} catch (TransformerException e) {
			System.out.println("failed in XSLT due to " + e.getMessage());
			e.printStackTrace();
		}
	}

	private String doXSLT(String trim) throws TransformerException {
		ClassLoader cl = this.getClass().getClassLoader();
		String systemID = "WEB-INF/classes/removeNamespaces.xsl";
		InputStream in = cl.getResourceAsStream(systemID);
		URL url = cl.getResource(systemID);
		Source source = new StreamSource(in);
		source.setSystemId(url.toExternalForm());
		TransformerFactory transFact = TransformerFactory.newInstance();
		Transformer trans = transFact.newTransformer(source);
		Source xmlInput = new StreamSource(new StringReader(trim));
		StringWriter sw = new StringWriter();
		Result result = new StreamResult(sw);
		trans.transform(xmlInput, result);
		return sw.toString();
	}

	@PreDestroy
	public void releaseKafkaResources() {
		this.kafkaProducer.close();
	}
}
