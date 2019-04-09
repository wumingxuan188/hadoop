package com.casic.ensure;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class SplitBolt implements IRichBolt {
    private TopologyContext context ;
    private OutputCollector collector ;
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.context = context ;
        this.collector = collector ;
    }
    public void execute(Tuple tuple) {
        String line = tuple.getString(0);
       //System.err.println(this+":execute():"+line);
        String[] arr = line.split(" ");
        for(String s : arr){
            collector.emit(new Values(s,1));
        }
        if(new Random().nextBoolean()){
            collector.ack(tuple);
            System.err.println(this+":ack:"+line);
        }else {
            collector.fail(tuple);
            System.err.println(this+":fail:"+line);
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
