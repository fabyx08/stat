package fourth_query;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import utils.Median;

import java.util.HashMap;
import java.util.Map;

import static constants.Constants.NOT_FOUND;
import static constants.StormParams.S_METRONOME;
import static constants.TupleFields.*;


public class SensorPQuantile extends BaseRichBolt {

    private HashMap<String, Median> userQuantile;
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.userQuantile = new HashMap<String, Median> ();
        //this.lampRoad = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(S_METRONOME)) {
            handleCountLampForRoad(tuple);
        } else {
            handleTuple(tuple);
        }
        collector.ack(tuple);
    }

    private void handleCountLampForRoad(Tuple tuple) {
        long timestamp = tuple.getLongByField(TIMESTAMP);
        for (String id : userQuantile.keySet()) {
            Median median = userQuantile.get(id);
            double estimatedQuantile = median.publish();
            if (estimatedQuantile == NOT_FOUND) {
                return;
            }
            //Values values = new Values(id, address, estimatedQuantile, timestamp);
            //collector.emit(values);
        }
        userQuantile.clear();
    }

    private void handleTuple(Tuple tuple) {
        String id = tuple.getStringByField(ID);
        String sensor = tuple.getStringByField(SENSOR);
        double currentValue = tuple.getDoubleByField(CURRENT_VALUE);
        Median median = userQuantile.get(id);
        if (median == null) {
            median = new Median(sensor);
            userQuantile.put(id, median);
            median.observer(currentValue);
        } else {
            median.observer(currentValue);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields(ID, ADDRESS, PQUANTILE, TIMESTAMP));
    }
}