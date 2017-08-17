package third_query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import utils.Window;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static constants.Constants.MILL_IN_HOUR;
import static constants.Constants.WINDOW_SIZE_HOUR;
import static constants.KafkaParams.KAFKA_IP_PORT;
import static constants.KafkaParams.MONITORING_QUERY3_LAMP_HOURLY;
import static constants.StormParams.S_METRONOME;
import static constants.TupleFields.*;

public class ComputeMeanSensorHourly extends BaseRichBolt {

    /**
     * The purpose of the class is to determine the average consume for a single hour of every single user.
     */
    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private Map<String, Window> userMap;
    private long lastMetronomeTimestamp; //Last instant in which it is received a tuple from Metronome
    private KafkaProducer<String, String> producer;
    private ObjectMapper mapper;


    @Override
    public void prepare(Map map, TopologyContext topology, OutputCollector collector) {

        this.collector = collector;
        this.userMap = new HashMap<String, Window>();
        this.lastMetronomeTimestamp = 0;
        this.mapper = new ObjectMapper();

        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_IP_PORT);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);

        producer = new KafkaProducer<String, String>(props);
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(S_METRONOME)) {
            //Tuple from Metronome
            handleMetronomeMessage(tuple);
        } else {
            handleSensorReport(tuple);
        }
        collector.ack(tuple);
    }

    private void handleMetronomeMessage(Tuple tuple) {

        long tupleTimestamp = tuple.getLongByField(TIMESTAMP);

        /*Consider only the tuples belonging to the current hour */
        if (tupleTimestamp > this.lastMetronomeTimestamp) {
            //Determine the number of hours spent
            int elapsedHour = (int) Math.ceil((tupleTimestamp - lastMetronomeTimestamp) / MILL_IN_HOUR);


            for (String id : userMap.keySet()) {

                Window w = userMap.get(id);
                int sampleSize = w.getEstimatedTotal(); //sample sum
                double sampleSum = w.getEstimatedSum(); //sample size
                // Advancement time window
                w.moveForward(elapsedHour);

                Double lampHourMean = sampleSum / sampleSize;

                //Values v = new Values(id, w.getSensor(), sampleSize, sampleSum, tupleTimestamp);
                //collector.emit(v);

                ObjectNode objectNode = mapper.createObjectNode();
                objectNode.put(ID, id);
                objectNode.put(SENSOR, w.getSensor());
                objectNode.put(TIMESTAMP, tupleTimestamp);
                objectNode.put(SAMPLE_SIZE, sampleSize);
                objectNode.put(SAMPLE_SUM, sampleSum);
                objectNode.put(MEAN, lampHourMean);

                producer.send(new ProducerRecord<String, String>(MONITORING_QUERY3_LAMP_HOURLY, objectNode.toString()));

            }
            this.lastMetronomeTimestamp = tupleTimestamp; //Update
        }
    }

    private void handleSensorReport(Tuple tuple) {

        String id = tuple.getStringByField(ID);
        String sensor = tuple.getStringByField(SENSOR);
        long tupleTimestamp = tuple.getLongByField(TIMESTAMP);

        if (tupleTimestamp > this.lastMetronomeTimestamp) {

            Window w = userMap.get(id);
            if (w == null) {
                //Add streetlamp
                w = new Window(WINDOW_SIZE_HOUR);
                w.setSensor(sensor);
                userMap.put(id, w);
            }
            w.increment(tuple.getDoubleByField(CURRENT_VALUE)); //Add consume
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //outputFieldsDeclarer.declare(new Fields(ID, SENSOR, SAMPLE_SIZE, SAMPLE_SUM, TIMESTAMP));
    }
}
