package com.data.jstorm.kafka;

import com.alibaba.jstorm.utils.JStormUtils;

import java.io.Serializable;
import java.util.*;


public class KafkaSpoutConfig implements Serializable {


    private static final long serialVersionUID = 1L;
    public List<Host> brokers;
    public int numPartitions;
    public String topic;
    public String zkRoot;
    public List<Host> zkServers;
    public int fetchMaxBytes = 256 * 1024;
    public int fetchWaitMaxMs = 10000;
    public int socketTimeoutMs = 30 * 1000;
    public int socketReceiveBufferBytes = 64 * 1024;
    public long startOffsetTime = -1;
    public boolean fromBeginning = false;
    public String clientId;
    public boolean resetOffsetIfOutOfRange = false;
    public long offsetUpdateIntervalMs = 2000;
    private Properties properties = null;
    private Map stormConf;
    public String zkHosts;
    public String brokerHosts;
    public int batchSendCount = 1;
    public boolean forceFromStart = false;
    private HashMap<String, Integer> hosts;

    public int getNumPartitions() {
        return numPartitions;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public String getZkRoot() {
        return zkRoot;
    }

    public void setZkRoot(String zkRoot) {
        this.zkRoot = zkRoot;
    }

    public int getFetchMaxBytes() {
        return fetchMaxBytes;
    }

    public void setFetchMaxBytes(int fetchMaxBytes) {
        this.fetchMaxBytes = fetchMaxBytes;
    }

    public int getFetchWaitMaxMs() {
        return fetchWaitMaxMs;
    }

    public void setFetchWaitMaxMs(int fetchWaitMaxMs) {
        this.fetchWaitMaxMs = fetchWaitMaxMs;
    }

    public int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }

    public void setSocketTimeoutMs(int socketTimeoutMs) {
        this.socketTimeoutMs = socketTimeoutMs;
    }

    public int getSocketReceiveBufferBytes() {
        return socketReceiveBufferBytes;
    }

    public void setSocketReceiveBufferBytes(int socketReceiveBufferBytes) {

        this.socketReceiveBufferBytes = socketReceiveBufferBytes;
    }

    public long getStartOffsetTime() {

        return startOffsetTime;
    }

    public void setStartOffsetTime(long startOffsetTime) {
        this.startOffsetTime = startOffsetTime;
    }

    public boolean isFromBeginning() {
        return fromBeginning;
    }

    public void setFromBeginning(boolean fromBeginning) {
        this.fromBeginning = fromBeginning;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public boolean isResetOffsetIfOutOfRange() {
        return resetOffsetIfOutOfRange;
    }

    public void setResetOffsetIfOutOfRange(boolean resetOffsetIfOutOfRange) {
        this.resetOffsetIfOutOfRange = resetOffsetIfOutOfRange;
    }

    public long getOffsetUpdateIntervalMs() {
        return offsetUpdateIntervalMs;
    }

    public void setOffsetUpdateIntervalMs(long offsetUpdateIntervalMs) {
        this.offsetUpdateIntervalMs = offsetUpdateIntervalMs;
    }

    public String getZkHosts() {
        return zkHosts;
    }

    public void setZkHosts(String zkHosts) {
        this.zkHosts = zkHosts;
    }

    public String getBrokerHosts() {
        return brokerHosts;
    }

    public void setBrokerHosts(String brokerHosts) {
        this.brokerHosts = brokerHosts;
    }

    public int getBatchSendCount() {
        return batchSendCount;
    }

    public void setBatchSendCount(int batchSendCount) {
        this.batchSendCount = batchSendCount;
    }

    public boolean isForceFromStart() {
        return forceFromStart;
    }

    public void setForceFromStart(boolean forceFromStart) {
        this.forceFromStart = forceFromStart;
    }

    public KafkaSpoutConfig() {

    }


    public KafkaSpoutConfig(Properties properties) {
        this.properties = properties;
    }

    public void configure(Map conf) {

        this.stormConf = conf;
        topic = getConfig("kafka.topic", "jstorm");
        zkRoot = getConfig("storm.zookeeper.root", "/jstorm");

        String zkHosts = getConfig("kafka.zookeeper.hosts", "127.0.0.1:2181");
        zkServers = convertHosts(zkHosts, 2181);
        String brokerHosts = getConfig("kafka.broker.hosts", "127.0.0.1:9092");
        brokers = convertHosts(brokerHosts, 9092);
        numPartitions = JStormUtils.parseInt(getConfig("kafka.broker.partitions"), 1);
        fetchMaxBytes = JStormUtils.parseInt(getConfig("kafka.fetch.max.bytes"), 256*1024);
        fetchWaitMaxMs = JStormUtils.parseInt(getConfig("kafka.fetch.wait.max.ms"), 10000);
        socketTimeoutMs = JStormUtils.parseInt(getConfig("kafka.socket.timeout.ms"), 30 * 1000);
        socketReceiveBufferBytes = JStormUtils.parseInt(getConfig("kafka.socket.receive.buffer.bytes"), 64*1024);
        fromBeginning = JStormUtils.parseBoolean(getConfig("kafka.fetch.from.beginning"), false);
        startOffsetTime = JStormUtils.parseInt(getConfig("kafka.start.offset.time"), -1);
        offsetUpdateIntervalMs = JStormUtils.parseInt(getConfig("kafka.offset.update.interval.ms"), 2000);
        clientId = getConfig("kafka.client.id", "jstorm");
        batchSendCount = JStormUtils.parseInt(getConfig("kafka.spout.batch.send.count"), 1);
//        numPartitions = JStormUtils.parseInt(getConfig("kafka.broker.partitions"), KafkaInfo.getTopicNumPartitions(topic, hosts, clientId));
    }

    /*
     * 配置完成后 需要初始化
     */
    public void initconfigure() {

        if (!"jstorm".equals(getTopic()) && getTopic() != null) {
            topic = getTopic();
        } else {
            topic = getConfig("kafka.topic", "jstorm");
        }
        if (getZkHosts() != null && !getZkHosts().contains("127.0.0.1")) {
            zkHosts = getZkHosts();
        } else {
            zkHosts = getConfig("kafka.zookeeper.hosts", "127.0.0.1:2181");
        }
        zkRoot = getConfig("storm.zookeeper.root", "/jstorm");
        zkServers = convertHosts(zkHosts, 2181);
        if (getBrokerHosts() != null && !getBrokerHosts().contains("127.0.0.1")) {
            brokerHosts = getBrokerHosts();
        } else {
            brokerHosts = getConfig("kafka.broker.hosts", "127.0.0.1:9092");
        }
        brokers = convertHosts(brokerHosts, 9092);
        hosts = mapHosts(brokerHosts, 9092);
        if (getFetchMaxBytes() != 0 && getFetchMaxBytes() != 262144) {
            fetchMaxBytes = getFetchMaxBytes();
        } else {

            fetchMaxBytes = JStormUtils.parseInt(getConfig("kafka.fetch.max.bytes"), 256 * 1024);
        }

        if (getFetchWaitMaxMs() != 0 && getFetchWaitMaxMs() != 10000) {
            fetchWaitMaxMs = getFetchWaitMaxMs();
        } else {
            fetchWaitMaxMs = JStormUtils.parseInt(getConfig("kafka.fetch.wait.max.ms"), 10000);
        }
        if (getSocketTimeoutMs() != 0 && getSocketTimeoutMs() != 30000) {
            socketTimeoutMs = getSocketTimeoutMs();
        } else {
            socketTimeoutMs = JStormUtils.parseInt(getConfig("kafka.socket.timeout.ms"), 30 * 1000);
        }
        if (getSocketReceiveBufferBytes() != 0 && getSocketReceiveBufferBytes() != 65536) {
            socketReceiveBufferBytes = getSocketReceiveBufferBytes();
        } else {
            socketReceiveBufferBytes = JStormUtils.parseInt(getConfig("kafka.socket.receive.buffer.bytes"), 64 * 1024);
        }
        if (isFromBeginning() != false) {
            fromBeginning = isFromBeginning();
        } else {
            fromBeginning = JStormUtils.parseBoolean(getConfig("kafka.fetch.from.beginning"), false);
        }
        if (getStartOffsetTime() != -1) {
            startOffsetTime = getStartOffsetTime();
        } else {
            startOffsetTime = JStormUtils.parseInt(getConfig("kafka.start.offset.time"), -1);
        }
        if (getOffsetUpdateIntervalMs() != 2000) {
            offsetUpdateIntervalMs = getOffsetUpdateIntervalMs();
        } else {
            offsetUpdateIntervalMs = JStormUtils.parseInt(getConfig("kafka.offset.update.interval.ms"), 2000);
        }
        if (getClientId() != null && "jstorm".equals(getClientId())) {
            clientId = getClientId();
        } else {
            clientId = getConfig("kafka.client.id", "jstorm");
        }
        if (getBatchSendCount() != 1) {
            batchSendCount = getBatchSendCount();
        } else {
            batchSendCount = JStormUtils.parseInt(getConfig("kafka.spout.batch.send.count"), 1);
        }
        if (getNumPartitions() != 1 && getNumPartitions() != 0) {
            numPartitions = getNumPartitions();
        } else {
            numPartitions = JStormUtils.parseInt(getConfig("kafka.broker.partitions"), KafkaInfo.getTopicNumPartitions(topic, hosts, clientId));
        }

    }


    private String getConfig(String key) {
        return getConfig(key, null);
    }

    private String getConfig(String key, String defaultValue) {
        if (properties != null && properties.containsKey(key)) {
            return properties.getProperty(key);
        } else if (stormConf != null && stormConf.containsKey(key)) {
            return String.valueOf(stormConf.get(key));
        } else {
            return defaultValue;
        }
    }


    public List<Host> convertHosts(String hosts, int defaultPort) {
        List<Host> hostList = new ArrayList<Host>();
        String[] hostArr = hosts.split(",");
        for (String s : hostArr) {
            Host host;
            String[] spec = s.split(":");
            if (spec.length == 1) {
                host = new Host(spec[0], defaultPort);
            } else if (spec.length == 2) {
                host = new Host(spec[0], JStormUtils.parseInt(spec[1]));
            } else {
                throw new IllegalArgumentException("Invalid host specification: " + s);
            }
            hostList.add(host);
        }
        return hostList;
    }

    public HashMap<String, Integer> mapHosts(String ipSting, int defaultPort) {
        hosts = new HashMap<>();
        String[] hostArr = ipSting.split(",");
        for (String s : hostArr) {
            String[] spec = s.split(":");
            if (spec.length == 1) {
                hosts.put(spec[0], defaultPort);
            } else if (spec.length == 2) {
                hosts.put(spec[0], JStormUtils.parseInt(spec[1]));
            } else {
                throw new IllegalArgumentException("Invalid host specification: " + s);
            }
        }
        return hosts;
    }

    public List<Host> getHosts() {
        return brokers;
    }

    public void setHosts(List<Host> hosts) {
        this.brokers = hosts;
    }

    public int getPartitionsPerBroker() {
        return numPartitions;
    }

    public void setPartitionsPerBroker(int partitionsPerBroker) {
        this.numPartitions = partitionsPerBroker;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public static void main(String[] args) {

        KafkaSpoutConfig kc = new KafkaSpoutConfig();
        kc.topic = "bjnews-load";
        System.out.println(kc.numPartitions);
    }

}
