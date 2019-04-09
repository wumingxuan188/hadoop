package com.casic;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class CallLogCreateBolt implements IRichBolt {
    private  OutputCollector collector;
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }
    public void execute(Tuple input) {
        String from = input.getString(0);
        String to = input.getString(1);
        Integer duration = input.getInteger(2);
        collector.emit(new Values(from+"-"+to,duration));
    }
    public void cleanup() {
    }
    /**
     * 设置输出名称
     * @param declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("call","duration"));
    }
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
