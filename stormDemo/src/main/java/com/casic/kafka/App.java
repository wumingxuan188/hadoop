package com.casic.kafka;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.UUID;

public class App {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        //设置zk连接地址
        String zkConnString="192.168.116.201:2181";
        BrokerHosts hosts = new ZkHosts(zkConnString);
        //Spout配置(主题地址,和主题在zk上的路径)
        SpoutConfig spoutConfig = new SpoutConfig(hosts, "test2", "/test2", UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
       //创建一个kafka 源头
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        builder.setSpout("kafka-spout",kafkaSpout).setNumTasks(2);
        //设置creator-Bolt
        builder.setBolt("split-bolt", new SplitBolt(),2).allGrouping("kafka-spout").setNumTasks(2);
        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.setDebug(true);
        /**
         * 本地模式storm
         */
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wc", conf, builder.createTopology());

    }
}
