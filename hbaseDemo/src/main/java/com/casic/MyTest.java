package com.casic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MyTest {

    @Test
    public void test1(){
       // int i = 31 & Integer.MAX_VALUE;
        int i = 11<< 2;
        System.err.println(i);
    }


    @Test
    public void put() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:callLogs");
        Table table = conn.getTable(tname);

        String callerId = "13845456767" ;
        String calleeId = "139898987878" ;
        SimpleDateFormat sdf = new SimpleDateFormat();
        sdf.applyPattern("yyyyMMddHHmmss");
        String callTime = sdf.format(new Date());
        int duration = 100 ;
        DecimalFormat dff = new DecimalFormat();
        dff.applyPattern("00000");
        String durStr = dff.format(duration);

        //区域00-99
        int hash = (callerId + callTime.substring(0, 6)).hashCode();
        hash = (hash & Integer.MAX_VALUE) % 100 ;

        //hash区域号
        DecimalFormat df = new DecimalFormat();
        df.applyPattern("00");
        String regNo = df.format(hash);

        //拼接rowkey
        //xx , callerid , time ,  direction, calleid  ,duration
        String rowkey = regNo + "," + callerId + "," + callTime + "," + "0," + calleeId + "," + durStr  ;
        byte[] rowid = Bytes.toBytes(rowkey);
        Put put = new Put(rowid);
        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("callerPos"),Bytes.toBytes("河北"));
        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("calleePos"),Bytes.toBytes("河南"));
        //执行插入
        table.put(put);
        System.out.println("over");
    }
}
