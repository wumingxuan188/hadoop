package com.casic.group.shuffer;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class App {
    public static void main(String[] args) {
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("wc-spout",new WordCountSpout()).setNumTasks(2);
        builder.setBolt("wc-split",new WordCountSplitBolt()).shuffleGrouping("wc-spout").setNumTasks(2);
        builder.setBolt("wc-count1",new WordCounTerBolt()).shuffleGrouping("wc-split").setNumTasks(2);
        builder.setBolt("wc-count2",new WordCounTerBolt()).fieldsGrouping("wc-count1",new Fields("word")).setNumTasks(2);
        Config config = new Config();
        config.setNumWorkers(2);
        config.setDebug(true);
        LocalCluster localCluster=new LocalCluster();
        localCluster.submitTopology("wc",config,builder.createTopology());
    }
}
