package com.data.jstorm.kafka;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.google.common.collect.ImmutableMap;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;


/**
 * 
 * @author feilaoda
 *
 */

public class PartitionConsumer {
    private static Logger LOG = LoggerFactory.getLogger(PartitionConsumer.class);

    static enum EmitState {
        EMIT_MORE, EMIT_END, EMIT_NONE
    }

    private int partition;
    private KafkaConsumer consumer;


    private PartitionCoordinator coordinator;

    private KafkaSpoutConfig config;
    private ConcurrentLinkedQueue<MessageAndOffset> emittingMessages = new ConcurrentLinkedQueue<MessageAndOffset>();
    private SortedSet<Long> pendingOffsets = new ConcurrentSkipListSet<Long>();
    private SortedSet<Long> failedOffsets = new ConcurrentSkipListSet<Long>();
    private long emittingOffset;
    private long lastCommittedOffset;
    private ZkState zkState;
    private Map stormConf;

    public PartitionConsumer(Map conf, KafkaSpoutConfig config, int partition, ZkState offsetState) {
        this.stormConf = conf;
        this.config = config;
        this.partition = partition;
        this.consumer = new KafkaConsumer(config);
        this.zkState = offsetState;

        Long jsonOffset = null;
        try {
            Map<Object, Object> json = offsetState.readJSON(zkPath());
            if (json != null) {
                // jsonTopologyId = (String)((Map<Object,Object>)json.get("topology"));
                jsonOffset = (Long) json.get("offset");
            }
        } catch (Throwable e) {
            LOG.warn("Error reading and/or parsing at ZkNode: " + zkPath(), e);
        }

        try {
            if (config.fromBeginning) {
                emittingOffset = consumer.getOffset(config.topic, partition, kafka.api.OffsetRequest.EarliestTime());
            } else {
                if (jsonOffset == null) {
                    lastCommittedOffset = consumer.getOffset(config.topic, partition, kafka.api.OffsetRequest.LatestTime());
                } else {
                    lastCommittedOffset = jsonOffset;
                }
                emittingOffset = lastCommittedOffset;
            }
        } catch (Exception e) {
            LOG.error("error PartitionConsumer {},{}",e.getMessage(), e);
        }
    }

    public  EmitState emit(SpoutOutputCollector collector) {

        if (emittingMessages.isEmpty()) {
            fillMessages();
        }

        int count = 0;
        while (true) {
//            MessageAndOffset toEmitMsg = emittingMessages.pollFirst();
            MessageAndOffset toEmitMsg = emittingMessages.poll();
            if (toEmitMsg == null) {
                return EmitState.EMIT_END;
            }
            count ++;
            Iterable<List<Object>> tups = generateTuples(toEmitMsg.message());

            if (tups != null) {
                for (List<Object> tuple : tups) {
                    LOG.debug("emit message {}", new String(Utils.toByteArray(toEmitMsg.message().payload())));
                    collector.emit(tuple, new KafkaMessageId(partition, toEmitMsg.offset()));

                }
                if(count>=config.batchSendCount) {
                    break;
                }
            } else {
                ack(toEmitMsg.offset());
            }
        }

        if (emittingMessages.isEmpty()) {
            return EmitState.EMIT_END;
        } else {
            return EmitState.EMIT_MORE;
        }
    }

    private void fillMessages() {

        ByteBufferMessageSet msgs;
        Long minOffset=0L;
        try {
            long start = System.currentTimeMillis();
            msgs = consumer.fetchMessages(partition, emittingOffset + 1);
            if (msgs == null) {
                //add by fanwt
                minOffset = consumer.getOffset(config.topic, partition, kafka.api.OffsetRequest.EarliestTime());
                if (minOffset < emittingOffset){
                    LOG.error("fetch null message from offset {}", emittingOffset);
                    return;
                } else {
                    LOG.info("fetch message partition:{} offset before:{} laststart:{} ", partition, emittingOffset, minOffset);
                    emittingOffset = minOffset;
                    msgs = consumer.fetchMessages(partition, emittingOffset + 1);

//                    if(minOffset >= pendingOffsets.last()) {
//                        LOG.info("fetch message partition:{} offset before:{} laststart:{} ", partition, emittingOffset, minOffset);
//                        emittingOffset = minOffset;
//                        msgs = consumer.fetchMessages(partition, emittingOffset + 1);
//                    }else {
//                        LOG.info("fetch message partition:{} offset before:{}, start lastoffsets:{} ", partition, emittingOffset, pendingOffsets.last());
//                        emittingOffset = pendingOffsets.last();
//                        msgs = consumer.fetchMessages(partition, emittingOffset + 1);
//                    }
                }
            }
            
            int count = 0;
            for (MessageAndOffset msg : msgs) {
                count += 1;
                emittingMessages.add(msg);
                emittingOffset = msg.offset();
                pendingOffsets.add(emittingOffset);

                if(count % 1000 == 0){
                LOG.info("fillMessage fetched a message:{}, offset:{},offsetsize:{}", msg.message().toString(), msg.offset(),pendingOffsets.size());
                }
            }
            long end = System.currentTimeMillis();
//            LOG.info("add message pendingOffsets size:{}",pendingOffsets.size());
//            LOG.info("fetch message from partition:"+partition+", offset:" + emittingOffset+", size:"+msgs.sizeInBytes()+", count:"+count +", time:"+(end-start));
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage(),e);
        }
    }


    public synchronized void commitState() {
        try {
            long lastOffset = 0;
            if (pendingOffsets.isEmpty() || pendingOffsets.size() <= 0) {
                lastOffset = emittingOffset;
            } else {
                if (JStormUtils.parseInt(stormConf.get(Config.TOPOLOGY_ACKER_EXECUTORS), 0) == 0
                        && pendingOffsets.size() > 0) {
                    lastOffset = pendingOffsets.last();

                } else {
                    lastOffset = pendingOffsets.first();
                }
            }
            if (lastOffset != lastCommittedOffset) {
                Map<Object, Object> data = new HashMap<Object, Object>();
                data.put("topology", stormConf.get(Config.TOPOLOGY_NAME));
                data.put("offset", lastOffset);
                data.put("partition", partition);
                data.put("broker", ImmutableMap.of("host", consumer.getLeaderBroker().host(), "port", consumer.getLeaderBroker().port()));
                data.put("topic", config.topic);
                zkState.writeJSON(zkPath(), data);
                lastCommittedOffset = lastOffset;

                //add ack
                if(JStormUtils.parseInt(stormConf.get(Config.TOPOLOGY_ACKER_EXECUTORS),0) == 0
                        && pendingOffsets.size()>0){
//                    pendingOffsets.remove(minOffset);
                    if(pendingOffsets.last()<= lastOffset){
//                        LOG.info("This commit Offsets {}, Will be cleared size {}", lastOffset, pendingOffsets.size());
                        pendingOffsets.clear();
                    }else {
                        for(int i = 0 ; i<pendingOffsets.size(); i++ ){
                            Long tp =pendingOffsets.first();
                            if(tp<lastOffset){
                                pendingOffsets.remove(tp);
                            }
                          }
                        LOG.info("This commit offsets {}, lastsize {}",lastOffset,pendingOffsets.size());
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("commitzk error:{},{}",e.getMessage(), e);
        }

    }


    public void ack(long offset) {
        try {

            pendingOffsets.remove(offset);
//            LOG.info("pendingOffsets size:{},remove:{}",pendingOffsets.size(),offset);
        } catch (Exception e) {
            LOG.error("offset ack error " + offset);
        }
    }

    public void fail(long offset) {
        failedOffsets.remove(offset);
    }

    public void close() {
        coordinator.removeConsumer(partition);
        consumer.close();
    }

    @SuppressWarnings("unchecked")
    public Iterable<List<Object>> generateTuples(Message msg) {
        Iterable<List<Object>> tups = null;
        ByteBuffer payload = msg.payload();
        if (payload == null) {
            return null;
        }
        tups = Arrays.asList(Utils.tuple(Utils.toByteArray(payload)));
        return tups;
    }

    private String zkPath() {
        return config.zkRoot + "/kafka/offset/topic/" + config.topic + "/" + config.clientId + "/" + partition;
    }

    public PartitionCoordinator getCoordinator() {
        return coordinator;
    }

    public void setCoordinator(PartitionCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public KafkaConsumer getConsumer() {
        return consumer;
    }

    public void setConsumer(KafkaConsumer consumer) {
        this.consumer = consumer;
    }
}
