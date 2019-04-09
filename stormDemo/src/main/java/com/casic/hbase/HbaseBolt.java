package com.casic.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Map;

public class HbaseBolt implements IRichBolt {
    //全局变量
    private Table table;
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            //创建一个配置
            Configuration conf = HBaseConfiguration.create();
            //创建一个连接
            Connection conn = ConnectionFactory.createConnection(conf);
            TableName name = TableName.valueOf("ns1:wordcount");
            //获取一个表
            table = conn.getTable(name);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        Integer count = tuple.getInteger(1);
        //列族名称
        byte[] f = Bytes.toBytes("f1");
        //rowkey名称
        byte[] row = Bytes.toBytes(word);
        //列名称
        byte[] q = Bytes.toBytes("count");
        try {
            table.incrementColumnValue(row, f, q, count);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void cleanup() {
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
