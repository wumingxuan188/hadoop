package com.casic.ensure;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.*;

public class WordCountSpout implements IRichSpout {
    private TopologyContext context ;
    private SpoutOutputCollector collector ;
    private List<String> states ;
    private Random r = new Random();
    private int index = 0;
    //存储失败的次数
    private Map<Long,Integer> failMassage=new HashMap<Long, Integer>();
    //存储消息的集合
    private  Map<Long,String> massage=new HashMap<Long, String>();
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.context = context ;
        this.collector = collector ;
        states = new ArrayList<String>();
        states.add("hello world tom");
        states.add("hello world tomas");
        states.add("hello world tomasLee");
        states.add("hello world tomson");
    }
    public void close() {
    }
    public void activate() {
    }
    public void deactivate() {
    }
    public void nextTuple() {
        if(index < 3){
            String line = states.get(r.nextInt(4));
            long ts = System.currentTimeMillis();
            collector.emit(new Values(line),ts);
            System.out.println(this+": nextTuple : "+ts+line);
            massage.put(ts,line);
            index ++ ;
        }
    }
    public void ack(Object msgId) {
        //System.err.println(this+": ack :"+msgId);
        long ts=(Long) msgId;
        failMassage.remove(ts);
        massage.remove(ts);
       // System.err.println(this+":ack():"+ts+massage.get(ts));
    }
    public void fail(Object msgId) {
        long ts=(Long) msgId;
       //获取发送次数
        Integer count = failMassage.get(ts);
        count=count==null? 0:count;
        if(count<=3){
            //数据重试
            collector.emit(new Values(massage.get(ts)),ts );
            failMassage.put(ts,count+1);
        }else {
            failMassage.remove(ts);
            massage.remove(ts);
        }
        System.err.println(this+":fail()"+ts+massage.get(ts));
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
