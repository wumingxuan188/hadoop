package com.casic.group.shuffer;

import com.casic.util.Util;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class WordCounTerBolt implements IRichBolt {
   private TopologyContext context;
   private OutputCollector collector;
   private Map<String,Integer> map;
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.context=context;
        this.collector=collector;
        map=new HashMap<String, Integer>();
        map= Collections.synchronizedMap(map);
        Thread t = new Thread() {
            @Override
            public void run() {
                emitData();
            }
        };
        t.setDaemon(true);
        t.start();
    }
    //清分map
    private  void emitData(){
        synchronized (map){
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                collector.emit(new Values(entry.getKey(),entry.getValue()));

            }
            map.clear();
    }
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    public void execute(Tuple input) {
        String word = input.getString(0);
        Util.sendInfo(this,word);
        Integer count = input.getInteger(1);
        if(!map.containsKey(word)){
            map.put(word,count);
        }else {
            Integer counter = map.get(word);
            map.put(word,count+counter);
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
