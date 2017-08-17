package third_query;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static constants.Constants.MILL_IN_DAY;
import static constants.Constants.MILL_IN_HOUR;
import static constants.StormParams.D_METRONOME;
import static constants.StormParams.S_METRONOME;
import static constants.TupleFields.TIMESTAMP;

public class Metronome extends BaseRichBolt {
    private OutputCollector collector;
    private long currentTime; //Last instant when metronome with granularity of an hour is clicked
    private long currentTimeDay; ////Last instant when metronome with granularity of a day is clicked

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.currentTime = 0;
        this.currentTimeDay = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        Long time = tuple.getLongByField(TIMESTAMP);

        /*Send tuple if it is passed at least one hour */
        if (this.currentTime < time && (time - this.currentTime) >= MILL_IN_HOUR) {
            this.currentTime = time;
            collector.emit(S_METRONOME, new Values(time));
        }
        /*Send tuple if it is passed at least one day */
        if (this.currentTimeDay < time && (time - this.currentTimeDay) >= MILL_IN_DAY) {
            this.currentTimeDay = time;
            collector.emit(D_METRONOME, new Values(time));
        }
        collector.ack(tuple);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(S_METRONOME, new Fields(TIMESTAMP));
        outputFieldsDeclarer.declareStream(D_METRONOME, new Fields(TIMESTAMP));
    }
}
