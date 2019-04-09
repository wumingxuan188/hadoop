package com.casic.group.shuffer;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class WordCountSpout implements IRichSpout {
    private  TopologyContext context;
    private SpoutOutputCollector collector;
    private List<String> dataList;
    private Random random=new Random();

    private int index = 0;
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.context=context;
        this.collector=collector;
        dataList=new ArrayList<String>();
        dataList.add("hello word tomcat");
        dataList.add("hello hadoop jack");
        dataList.add("hello hadoop tom");
        dataList.add("hello hadoop cafaka");
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void nextTuple() {
        if(index < 3){
            String line = dataList.get(random.nextInt(4));
            collector.emit(new Values(line));
            //Util.sendToLocalhost(this, line);
            index ++ ;
        }
    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
