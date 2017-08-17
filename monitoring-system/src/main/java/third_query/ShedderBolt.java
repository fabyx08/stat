package third_query;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import static constants.Constants.MAX_SIZE;
import static constants.Constants.MAX_UPDATE_TIME;
import static constants.TupleFields.ID;
import static constants.TupleFields.TIMESTAMP;


public class ShedderBolt extends BaseRichBolt {

    private Map<Integer, Long> sampleTimestamp;
    private int maxsize;
    private int currentSize;
    private int maxUpdateTime;
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.sampleTimestamp = new HashMap<Integer, Long>();
        this.maxsize = MAX_SIZE;
        this.maxUpdateTime = MAX_UPDATE_TIME;
        this.currentSize = 0;
    }

    @Override
    public void execute(Tuple tuple) {

        int id = tuple.getIntegerByField(ID);
        long timestamp = tuple.getLongByField(TIMESTAMP);
        boolean send = false;

        if (sampleTimestamp.get(id) == null) {  // check if the lamp is in the hashmap

            currentSize += 1;

            //if it isn't, and hashmap is under the MAX_SIZE (not full)

            if (currentSize < maxsize) {
                sampleTimestamp.put(id, timestamp);
                //add lamp with its timestamp
                send = true;
            } else {
                //tuple denied
                currentSize -= 1;
            }
        } else {
            //lamp is in the hashmap, update the hashmap value until MAX_UPDATE_TIME times
            maxUpdateTime += 1;

            if (maxUpdateTime < 6) {

                //update
                sampleTimestamp.remove(id);
                sampleTimestamp.put(id, timestamp);
                send = true;

            } else {

                //MAX_UPDATE_TIME complete, remove item from hashmap
                sampleTimestamp.remove(id);
                maxUpdateTime = 0;
                currentSize -= 1;
            }


        }


        if (send) {

            //send tuple with its timestamp
            Values values = new Values();
            values.add(timestamp);
            collector.emit(values);
            collector.ack(tuple);
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(TIMESTAMP));
    }
}
