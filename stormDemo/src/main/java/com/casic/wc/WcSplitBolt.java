package com.casic.wc;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class WcSplitBolt implements IRichBolt {
    private  TopologyContext context;
    private  OutputCollector collector;
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.context=context;
        this.collector=collector;
    }
    public void execute(Tuple tuple) {
       //获取spout传来的数据
        String metaData = tuple.getString(0);
        //进行切割
        String[] data = metaData.split(" ");
        for (String datum : data) {
            collector.emit(new Values(datum,1));
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    public void cleanup() {
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","count"));
    }
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
