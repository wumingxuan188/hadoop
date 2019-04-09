package com.casic.group.direct;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class App {
    public static void main(String[] args) {
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("wc-spout",new WcSpout()).setNumTasks(2);
        builder.setBolt("wc-split",new WcSplit(),2).directGrouping("wc-spout").setNumTasks(2);
        Config conf=new Config();
        conf.setNumWorkers(2);
        conf.setDebug(true);
        LocalCluster localCluster=new LocalCluster();
        localCluster.submitTopology("wc",conf,builder.createTopology());
        //StormSubmitter.submitTopology();
    }





}
