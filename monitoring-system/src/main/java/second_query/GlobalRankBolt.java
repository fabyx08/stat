package second_query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import constants.TupleFields;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import utils.RankItem;
import utils.Ranking;
import utils.TopKRanking;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static constants.KafkaParams.KAFKA_IP_PORT;
import static constants.KafkaParams.MONITORING_QUERY2;
import static constants.StormParams.UPDATE;
import static constants.TupleFields.*;

public class GlobalRankBolt extends BaseRichBolt {
    /**
     * The GlobalRank does the merge of partial rankings.
     * In the case where the lamps whose bulbs needed replacement and for which a new bulb is installed
     * or in the case where a fault has occurred, we proceed to their elimination in the rankings.
     */
    private OutputCollector collector;
    private TopKRanking ranking;
    private int topK;
    private KafkaProducer<String, String> producer;
    private ObjectMapper mapper;


    public GlobalRankBolt(int topK) {
        this.topK = topK;
    }


    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;
        this.ranking = new TopKRanking(topK);
        this.mapper = new ObjectMapper();

        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_IP_PORT);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);

        producer = new KafkaProducer<String, String>(props);
    }

    @Override
    public void execute(Tuple tuple) {
        boolean updated = false;

        if (tuple.getSourceStreamId().equals(UPDATE)) {
            Ranking partialRanking = (Ranking) tuple.getValueByField(TupleFields.PARTIAL_RANK);
            /*Publish the ranking only if updates have occurred*/
            for (RankItem item : partialRanking.getRanking()) {
                updated |= ranking.update(item);
            }

        } else {
            /*Delete from the list the streetlamps with broken lamps or lamps that no longer exceed the average life time*/
            RankItem rankItem = (RankItem) tuple.getValueByField(RANK_ITEM);
            if (ranking.indexOf(rankItem) < topK)
                updated = true;
            ranking.remove(rankItem);
        }

        /* Emit if the local topK is changed */
        if (updated) printRanking();

        collector.ack(tuple);
    }

    private void printRanking() {
        List<RankItem> globalTopK = ranking.getTopK().getRanking();
        long currentTime = System.currentTimeMillis();

        for (RankItem rankItem : globalTopK) {
            ObjectNode objectNode = mapper.createObjectNode();

            objectNode.put(ID, rankItem.getId());
            objectNode.put(CITY, rankItem.getCity());
            objectNode.put(ADDRESS, rankItem.getAddress());
            objectNode.put(KM, rankItem.getKm());
            objectNode.put(BULB_MODEL, rankItem.getModel());
            objectNode.put(TIME_DIFF, currentTime - (rankItem.getInstallationTimestamp() + rankItem.getMeanExpirationTime()));

            producer.send(new ProducerRecord<String, String>(MONITORING_QUERY2, objectNode.toString()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
