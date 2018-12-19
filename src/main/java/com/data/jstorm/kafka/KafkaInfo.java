package com.data.jstorm.kafka;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by bigdata on  2018/1/11
 *
 * for get  getTopicNumPartitions
 */

public class KafkaInfo {
    private static Logger LOG = LoggerFactory.getLogger(KafkaInfo.class);
    public  KafkaInfo(){

    };
    public  static  int   getTopicNumPartitions(String topic,Map<String,Integer> hosts, String clientID) {
        int timeOut = 30000;
        int bufferSize = 64 * 1000000;
        TreeMap<Integer, PartitionMetadata> map = new TreeMap<>();
        SimpleConsumer consumer = null;
        loop:
        for (Map.Entry<String, Integer> bootstrap : hosts.entrySet()) {
            try {
                consumer = new SimpleConsumer(bootstrap.getKey(), bootstrap.getValue(), timeOut, bufferSize, clientID);
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        map.put(part.partitionId(), part);
                    }
                }
            } catch (Exception e) {
                LOG.error("Error communicating with Broker [{}] to find Leader for [{}] Reason: ",hosts,topic,e);
            } finally {
                if (consumer != null)
                    consumer.close();
            }
        }
        LOG.info("{} Partition num is {} ",topic,map.size());
        return map.size();
    }


}
