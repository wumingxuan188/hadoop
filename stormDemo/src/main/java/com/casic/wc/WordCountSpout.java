package com.casic.wc;

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
    private TopologyContext context;
    private SpoutOutputCollector collector;
    private Random random;
    private List<String> dataList;
   //初始化赋值,手动加入数据想list
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.context = context;
        this.collector = collector;
        this.dataList = new ArrayList<String>();
        dataList.add("hello word tom");
        dataList.add("hello hadoop");
        dataList.add("hello storm tomas");
        dataList.add("hello kafka tom");
        this.random = new Random();
    }
    public void close() {
    }
    public void activate() {
    }
    public void deactivate() {
    }
    /**
     * 获取数据源,进行处理
     */
    public void nextTuple() {
        int index = random.nextInt(4);
        String line = dataList.get(index);
        collector.emit(new Values(line));
    }
    public void ack(Object msgId) {
    }
    public void fail(Object msgId) {
    }

    /**
     * 定义输出字段
     *
     * @param declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
