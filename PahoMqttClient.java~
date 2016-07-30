package com.spring.mqtt;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
@RestController
@EnableWebMvc
public class PahoMqttClient {

	static String broker;
	static String clientId;
	static String content;
	static boolean retained;
	@RequestMapping(value = "/{topic}", method = RequestMethod.GET)
	public void publishMessageToTopic(@PathVariable(value = "topic") String topic)
	{
		broker = "tcp://localhost:1883";
		clientId = "Paho Client";
		content = "RELAIS ON";
		retained = false;
		printConfiguration();
		try {
			MemoryPersistence persistence = new MemoryPersistence();
			MqttClient client = new MqttClient(broker, clientId, persistence);
			MqttConnectOptions opts = new MqttConnectOptions();
			opts.setConnectionTimeout(300);
			MqttMessage message = new MqttMessage(content.getBytes());
			message.setRetained(retained);
			client.connect(opts);
			System.out.println("Connected to broker");
			client.publish(topic, message);
			System.out.println("Message '" + content + "' published to topic '" + topic + "'");
			client.disconnect();
			System.out.println("Disconnected");
			System.exit(0);
		} catch (MqttException me) {
			me.printStackTrace();
		}
	}
	private void printConfiguration() {
		System.out.println("Setting broker to '" + broker + "'");
		System.out.println("Setting clientId to '" + clientId + "'");
		System.out.println("Setting content to '" + content + "'");
		System.out.println("Setting retained to " + retained);
		System.out.println();
	}
}
