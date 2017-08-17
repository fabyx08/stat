package bolt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jdk.nashorn.internal.ir.ObjectNode;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static constants.TupleFields.*;

public class ParserBolt extends BaseRichBolt {

    private OutputCollector collector;
    private ObjectMapper mapper;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
    }

    public void execute(Tuple tuple) {
        try {
            String raw_tuple = tuple.getStringByField(RAW_TUPLE);
            JsonNode jsonNode = mapper.readTree(raw_tuple);

            Long timestamp = tuple.getLongByField(TIMESTAMP);
            String id = jsonNode.get(ID).asText();
            String sensor = jsonNode.get(SENSOR).asText();
            Double currentValue = jsonNode.get(CURRENT_VALUE).asDouble();


            Values values = new Values(timestamp, id, sensor, currentValue);

            collector.emit(values);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            collector.ack(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TIMESTAMP, ID, SENSOR, CURRENT_VALUE));
    }
}