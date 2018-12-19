package com.data.jstorm.kafka;

import backtype.storm.task.TopologyContext;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PartitionCoordinator {
	private KafkaSpoutConfig config;
	private Map<Integer, PartitionConsumer> partitionConsumerMap;
	private ConcurrentLinkedQueue<PartitionConsumer> partitionConsumers;

	private ZkState zkState;
	public  PartitionCoordinator(Map conf, KafkaSpoutConfig config, TopologyContext context, ZkState zkState) {
		this.config = config;
		this.zkState = zkState; 
		partitionConsumers = new ConcurrentLinkedQueue<PartitionConsumer>();
		createPartitionConsumers(conf, context);
	}

	private  void createPartitionConsumers(Map conf, TopologyContext context) {
	    partitionConsumerMap = new ConcurrentHashMap<Integer, PartitionConsumer>();
        int taskSize = context.getComponentTasks(context.getThisComponentId()).size();
        for(int i=context.getThisTaskIndex(); i<config.numPartitions; i+=taskSize) {
            PartitionConsumer partitionConsumer = new PartitionConsumer(conf, config, i, zkState);
            partitionConsumers.add(partitionConsumer);
            partitionConsumerMap.put(i, partitionConsumer);
        }
	}

	public ConcurrentLinkedQueue<PartitionConsumer> getPartitionConsumers() {
		return partitionConsumers;
	}
	
	public PartitionConsumer getConsumer(int partition) {
		return partitionConsumerMap.get(partition);
	}
	
	public void removeConsumer(int partition) {
	    PartitionConsumer partitionConsumer = partitionConsumerMap.get(partition);
		partitionConsumers.remove(partitionConsumer);
		partitionConsumerMap.remove(partition);
	}
	
	
	 
}
