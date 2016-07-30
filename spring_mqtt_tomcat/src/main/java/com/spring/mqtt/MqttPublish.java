package com.spring.mqtt;

import java.io.IOException;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import com.sun.xml.internal.messaging.saaj.packaging.mime.MessagingException;

@RestController
@EnableWebMvc
public class MqttPublish {

	@SuppressWarnings("unused")
	@RequestMapping(value = "/{topic}", method = RequestMethod.GET)
	public void publishMessageToTopic(@PathVariable(value = "topic") String topic)
			throws MqttPersistenceException, IOException, MessagingException {
		String brokerURL = "tcp://localhost:1883";
		String clientID = "mqttpublish";
		// 0(only one time) 1(at least one time) 2(exactly one time)
		int qos = 2;
		String content = "RELAIS ON";
		boolean retained = true;
		try {
			MemoryPersistence persistence = new MemoryPersistence();
			MqttClient client = new MqttClient(brokerURL, clientID, persistence);
			if (client == null) {
				String error = "Could not publish message to topic, MQTT client has not been initialized";
				throw new MessagingException(error);
			} else {
				MqttConnectOptions connOpts = new MqttConnectOptions();
				connOpts.setCleanSession(false);
				connOpts.setConnectionTimeout(500);
				System.out.println("Connecting to broker url: " + brokerURL);
				client.connect(connOpts);
				if (client.isConnected()) {
					System.out.println("Client is Connected");
					System.out.println(topic + " topic is created");
					MqttMessage message = new MqttMessage(content.getBytes());
					message.setRetained(retained);
					message.setQos(qos);
					client.publish(topic, message);
					System.out.println("Message '" + content + "' published to topic '" + topic + "'");
					client.disconnect();
					System.out.println("Connection is Disconnected to broker");
					System.exit(0);
				}
			}
		} catch (MqttException me) {
			System.out.println("reason " + me.getReasonCode());
			System.out.println("msg " + me.getMessage().contains("Timed out waiting for a response from the server"));
			System.out.println("loc " + me.getLocalizedMessage());
			System.out.println("cause " + me.getCause());
			System.out.println("excep " + me);
			me.printStackTrace();
		}
	}
}
