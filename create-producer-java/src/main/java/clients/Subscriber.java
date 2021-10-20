package clients;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.UUID;

public class Subscriber implements MqttCallback {

    private KafkaProducer producer;

    private final int qos = 1;
    private String host = "ssl://mqtt.hsl.fi:8883";
    private String clientId = "MQTT-Java-Example";
    //"/hfp/v2/journey/ongoing/vp/tram/#"
    private String topic = "/hfp/v2/journey/ongoing/vp/#";
    private String kafka_topic = "vehicle-positions";
    private MqttClient client;

    public Subscriber(KafkaProducer producer) {
        this.producer = producer;
    }

    public void start() throws  MqttException {
        MqttConnectOptions conOpt = new MqttConnectOptions();
        conOpt.setCleanSession(true);

        final String uuid = UUID.randomUUID().toString().replace("-", "");

        String clientId = this.clientId + "-" + uuid;
        this.client = new MqttClient(this.host, clientId, new MemoryPersistence());
        this.client.setCallback(this);
        this.client.connect(conOpt);

        this.client.subscribe(this.topic, this.qos);
    }

    @Override
    public void connectionLost(Throwable cause) {
        System.out.println("### Connection lost because " + cause + " ###");
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        System.out.printf("[%s] %s%n", topic, new String(message.getPayload()));
        final String value = new String(message.getPayload());
        final ProducerRecord<String, String> record = new ProducerRecord<>(this.kafka_topic, topic, value);
        producer.send(record);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        System.out.println("### Delivery complete ###");
    }
}
