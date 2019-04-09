package com.casic;

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
public class CallLogSpout  implements IRichSpout {
    //spout输出收集器
    private SpoutOutputCollector collector;
    //上下文
    private  TopologyContext context;
    //是否完成
    private  boolean completed=false;
    //随机发生器
    private Random randomGenerator = new Random();

    private Integer idx = 0;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector=spoutOutputCollector;
        this.context=topologyContext;
    }
    public void close() {

    }
    public void activate() {

    }
    public void deactivate() {
    }
    public void nextTuple() {
        if (this.idx <= 1000) {
            List<String> mobileNumbers = new ArrayList<String>();
            mobileNumbers.add("1234123401");
            mobileNumbers.add("1234123402");
            mobileNumbers.add("1234123403");
            mobileNumbers.add("1234123404");
            Integer localIdx = 0;
            while (localIdx++ < 100 && this.idx++ < 1000) {
                //取出主叫
                String caller = mobileNumbers.get(randomGenerator.nextInt(4));
                //取出被叫
                String callee = mobileNumbers.get(randomGenerator.nextInt(4));
                while (caller == callee) {
                    //重新取出被叫
                    callee = mobileNumbers.get(randomGenerator.nextInt(4));
                }
                //模拟通话时长
                Integer duration = randomGenerator.nextInt(60);

                //输出元组
                this.collector.emit(new Values(caller, callee, duration));
            }
        }
    }
    public void ack(Object o) {
    }

    public void fail(Object o) {
    }
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("from","to","duration"));
    }
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
