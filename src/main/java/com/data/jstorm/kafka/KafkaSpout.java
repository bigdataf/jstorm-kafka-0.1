package com.data.jstorm.kafka;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class KafkaSpout implements IRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);

	protected SpoutOutputCollector collector;

	private long lastUpdateMs;
	PartitionCoordinator coordinator;
	
	private KafkaSpoutConfig config;
	
	private ZkState zkState;
	
	public KafkaSpout() {
	    
	}
	
	public KafkaSpout(KafkaSpoutConfig config) {
		this.config = config;
	}

	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		if (this.config == null) {
			config = new KafkaSpoutConfig();
			config.configure(conf);
		}
		zkState = new ZkState(conf, config);
		coordinator = new PartitionCoordinator(conf, config, context, zkState);
		lastUpdateMs = System.currentTimeMillis();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		commitState();
	    zkState.close();
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nextTuple() {
		try {
			Collection<PartitionConsumer> partitionConsumers = coordinator.getPartitionConsumers();
			for (PartitionConsumer consumer : partitionConsumers) {
				PartitionConsumer.EmitState state = consumer.emit(collector);
				LOG.debug("====== partition " + consumer.getPartition() + " emit message state is " + state);
//			if(state != EmitState.EMIT_MORE) {
//				currentPartitionIndex  = (currentPartitionIndex+1) % consumerSize;
//			}
//			if(state != EmitState.EMIT_NONE) {
//				break;
//			}
			}
			long now = System.currentTimeMillis();
			if ((now - lastUpdateMs) > config.offsetUpdateIntervalMs) {
				commitState();
			}
		}catch (Exception e){
			LOG.error("kafka Spout Exception:{}",e.getStackTrace());
		}
		
	}
	
	public void commitState() {
		try {
			lastUpdateMs = System.currentTimeMillis();
			for (PartitionConsumer consumer : coordinator.getPartitionConsumers()) {
				consumer.commitState();
			}
		}catch (Exception e){
			LOG.error("commitState error:{}",e.getMessage());
		}
		
	}

	@Override
	public void ack(Object msgId) {
		try {
		KafkaMessageId messageId = (KafkaMessageId)msgId;
		PartitionConsumer consumer = coordinator.getConsumer(messageId.getPartition());
		consumer.ack(messageId.getOffset());
		}catch (Exception e){
			LOG.error("ack error:{}",e.getMessage());
		}
	}

	@Override
	public void fail(Object msgId) {
		KafkaMessageId messageId = (KafkaMessageId)msgId;
		PartitionConsumer consumer = coordinator.getConsumer(messageId.getPartition());
		consumer.fail(messageId.getOffset());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("bytes"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}


	public static void main(String[] args) {
		KafkaSpout kafkaSpout = new KafkaSpout();
	}
}
