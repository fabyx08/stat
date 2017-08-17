package third_query;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static constants.TupleFields.*;

public class SelectorBolt3_4 extends BaseRichBolt {
    private OutputCollector collector;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {

        Long timestamp = tuple.getLongByField(TIMESTAMP);
        String id = tuple.getStringByField(ID);
        String sensor = tuple.getStringByField(SENSOR);
        Double value = tuple.getDoubleByField(CURRENT_VALUE);

        Values values = new Values(id, value, sensor, timestamp);
        collector.emit(values);
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(ID, CURRENT_VALUE, SENSOR, TIMESTAMP));
    }
}
