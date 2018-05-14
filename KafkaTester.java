import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import java.util.*;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

class KafkaTester {
	Properties prodProps;
	Properties consProps;
	String topic = "start" + new Random().nextInt();

	void setProps(Properties props) {
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
	}

	KafkaTester() {
		prodProps = new Properties();
		consProps = new Properties();
	
		setProps(prodProps);
		setProps(consProps);
	
		prodProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prodProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		consProps.put("group.id", "test");
		consProps.put("enable.auto.commit", "false");
		consProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		createTopic(topic);
	}

	void createTopic(String topic) {
        String zookeeperConnect = "localhost:2181,localhost:2181";
        int sessionTimeoutMs = 10 * 1000;
        int connectionTimeoutMs = 8 * 1000;

        ZkClient zkClient = new ZkClient(
            zookeeperConnect,
            sessionTimeoutMs,
            connectionTimeoutMs,
            ZKStringSerializer$.MODULE$);

       // Security for Kafka was added in Kafka 0.9.0.0
       boolean isSecureKafkaCluster = false;
       // ZkUtils for Kafka was used in Kafka 0.9.0.0 for the AdminUtils API
       ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);

       int partitions = 3;
       int replication = 1;

       // Add topic configuration here
       Properties topicConfig = new Properties();

       AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig, RackAwareMode.Disabled$.MODULE$);
       zkClient.close();
	}

	void start() {

		Thread c1 = new Thread(new TestConsumer(1));
		Thread c2 = new Thread(new TestConsumer(2));
		Thread c3 = new Thread(new TestConsumer(3));
		Thread p = new Thread(new TestProducer(1));

		try {
			c1.start();
			c2.start();
			c3.start();
//			Thread.sleep(1000);
			p.start();
			c1.join();
			c2.join();
			c3.join();
			p.join();
		} catch (InterruptedException e) {
			System.out.println("start interrupted");
		}
	}

	class TestProducer implements Runnable {
		int num;
		TestProducer(int num) {
			this.num = num;
		}
		public void run() {
			System.out.println("Running producer " + num);
			Producer<String, String> producer = new KafkaProducer<>(prodProps);
			for (int i = 0; i < 10000; i++) {
			    producer.send(new ProducerRecord<String, String>(topic, i%3, Integer.toString(i), Integer.toString(i)));
			}
			producer.close();
		}
	}

	class TestConsumer implements Runnable {
		int num;
		TestConsumer(int num) {
			this.num = num;
		}
		public void run() {
			System.out.println("Running consumer " + num);
			Consumer<String, String> consumer = new KafkaConsumer<>(consProps);
			List<ConsumerRecord> recordList = new ArrayList<>();
			consumer.subscribe(Arrays.asList(topic));

			int count = 0;
			while (count < 30) {
				ConsumerRecords<String, String> records = consumer.poll(1);
				for (Iterator<ConsumerRecord<String, String>> iter = records.iterator(); iter.hasNext();) {
					count++;
					ConsumerRecord r = iter.next();
					recordList.add(r);
				}
				if (count != 0 ) {
					System.out.println("consumer " + num + " got " + count + " records.");
				}
			}
			consumer.close();
			for (ConsumerRecord r : recordList) {
				System.out.println("consumer " + num + " got " + r.key() + ":" + r.value());
			}
		}
	}

	public static void main(String args[]) {
		KafkaTester kt = new KafkaTester();
		kt.start();
	}
}
