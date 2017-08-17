package spout;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static constants.KafkaParams.KAFKA_IP_PORT;
import static constants.KafkaParams.MONITORING_SOURCE;
import static constants.TupleFields.RAW_TUPLE;
import static constants.TupleFields.TIMESTAMP;

public class KafkaSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private KafkaConsumer<String, String> consumer;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_IP_PORT);
        props.put("group.id", "monitoring-system");
        props.put("enable.auto.commit", "true");
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", StringDeserializer.class);

        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList(MONITORING_SOURCE));
    }

    public void nextTuple() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {

                Long timestamp = record.timestamp();
                String tuple = record.value();

                Values values = new Values(timestamp, tuple);
                collector.emit(values);
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TIMESTAMP, RAW_TUPLE));
    }
}
