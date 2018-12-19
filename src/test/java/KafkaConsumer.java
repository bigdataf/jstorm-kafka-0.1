import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumer {

    private final ConsumerConnector consumer;

    public KafkaConsumer(ConsumerConnector consumer) {
        this.consumer = consumer;
    }

    public static void main(String[] args) {
    }
}