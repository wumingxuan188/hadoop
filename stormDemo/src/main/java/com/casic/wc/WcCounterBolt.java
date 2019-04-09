package com.casic.wc;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class WcCounterBolt implements IRichBolt {
    private TopologyContext context;
    private OutputCollector collector;
    private  Map<String,Integer> map;
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.context=context;
        this.collector=collector;
        this.map=new HashMap<String, Integer>();
    }
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        Integer count = tuple.getInteger(1);
        //若是一个词则直接放入map中,值是1
        if(!map.containsKey(word)){
            map.put(word,1);
        }else {
            //如果是第二次,则先取出值再加一
            map.put(word,map.get(word)+count);
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    //停止后遍历输出map
    public void cleanup() {
        for(Map.Entry<String,Integer> entry : map.entrySet()){
            System.err.println(entry.getKey()+":"+entry.getValue());
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
