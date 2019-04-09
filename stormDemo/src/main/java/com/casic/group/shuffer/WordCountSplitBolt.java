package com.casic.group.shuffer;

import com.casic.util.Util;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
public class WordCountSplitBolt implements IRichBolt {
    private   TopologyContext context;
    private  OutputCollector collector;
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
        this.context=context;
    }

    public void execute(Tuple input) {
        String line = input.getString(0);
        String[] words = line.split(" ");
        for (String word : words) {
            collector.emit(new Values(word,1));
            Util.sendInfo(this,word);
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
