package com.casic.wc;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WcApp {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
       //setNumTasks设置spout任务数,参数 parallelism_hint 设置线程数即executor个数
        //每个线程可以执行多个task.
        builder.setSpout("wc-spout",new WordCountSpout(),3).setNumTasks(4);
        builder.setBolt("wc-split-bolt",new WcSplitBolt(),3).shuffleGrouping("wc-spout").setNumTasks(3);
        builder.setBolt("wc-count-bolt",new WcCounterBolt(),4).
                fieldsGrouping("wc-split-bolt",new Fields("word")).setNumTasks(5);
        Config config = new Config();
       //设置work数量(work是进程)
        config.setNumWorkers(3);
        config.setDebug(true);
       /* LocalCluster localCluster=new LocalCluster();
        localCluster.submitTopology("mytop",config,builder.createTopology());
        Thread.sleep(10000);
        localCluster.shutdown();*/
       //集群方式提交
       StormSubmitter.submitTopology("wcTop",config,builder.createTopology());
    }
}
