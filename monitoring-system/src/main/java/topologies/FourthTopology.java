package topologies;

import bolt.ParserBolt;
import fourth_query.SensorPQuantile;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import spout.KafkaSpout;
import third_query.Metronome;
import third_query.SelectorBolt3_4;
import third_query.ShedderBolt;

import static constants.StormParams.*;
import static constants.TupleFields.ID;


public class FourthTopology {
    private final TopologyBuilder builder;

    public FourthTopology() {
        this.builder = new TopologyBuilder();

        builder.setSpout(KAFKA_SPOUT, new KafkaSpout()).setNumTasks(4);
        builder.setBolt(PARSER_BOLT, new ParserBolt()).shuffleGrouping(KAFKA_SPOUT).setNumTasks(4);
        builder.setBolt(SELECTOR_BOLT_3_4, new SelectorBolt3_4()).fieldsGrouping(PARSER_BOLT, new Fields(ID)).setNumTasks(4);
        builder.setBolt(SHEDDER, new ShedderBolt(), 1).allGrouping(PARSER_BOLT).setNumTasks(1);
        builder.setBolt(METRONOME, new Metronome(), 1).allGrouping(SHEDDER).setNumTasks(1);

        builder.setBolt(LAMP_PQUANTILE, new SensorPQuantile()).
                fieldsGrouping(SELECTOR_BOLT_3_4, new Fields(ID)).allGrouping(METRONOME, S_METRONOME).setNumTasks(4);

    }
    public static TopologyBuilder setTopology(TopologyBuilder builder) {
        if (builder == null) {
            builder = new TopologyBuilder();
            builder.setSpout(KAFKA_SPOUT, new KafkaSpout()).setNumTasks(4);
            builder.setBolt(PARSER_BOLT, new ParserBolt()).localOrShuffleGrouping(KAFKA_SPOUT).setNumTasks(4);
            builder.setBolt(SELECTOR_BOLT_3_4, new SelectorBolt3_4()).fieldsGrouping(PARSER_BOLT, new Fields(ID)).setNumTasks(4);
            builder.setBolt(SHEDDER, new ShedderBolt(), 1).allGrouping(PARSER_BOLT).setNumTasks(1);
            builder.setBolt(METRONOME, new Metronome(), 1).allGrouping(SHEDDER).setNumTasks(1);
        }

        builder.setBolt(LAMP_PQUANTILE, new SensorPQuantile()).
                fieldsGrouping(SELECTOR_BOLT_3_4, new Fields(ID)).allGrouping(METRONOME, S_METRONOME).setNumTasks(4);

        return builder;
    }

    public StormTopology createTopology() {
        return builder.createTopology();
    }
}
