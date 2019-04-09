package com.casic;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class App {
    public static void main(String[] args) throws Exception {
        //创建一个TopologyBuilder对对象
        TopologyBuilder builder = new TopologyBuilder();
        //设置spou源头
        builder.setSpout("spout",new CallLogSpout());
        //设置第一个bolt,确定从哪里取数据
        builder.setBolt("create-bolt",new CallLogCreateBolt()).shuffleGrouping("spout");
        //设置第二个bolt
        builder.setBolt("counter-bolt",new CallLogCounterBolt()).
                fieldsGrouping("create-bolt",new Fields("call"));
        Config config=new Config();
        config.setDebug(true);
       //本地版测试
        //LocalCluster localCluster=new LocalCluster();
        //localCluster.submitTopology("LogAnalyserStorm",config,builder.createTopology());
        //集群版测试
        StormSubmitter.submitTopology("mytop",config,builder.createTopology());
        Thread.sleep(100);
    }

    }

