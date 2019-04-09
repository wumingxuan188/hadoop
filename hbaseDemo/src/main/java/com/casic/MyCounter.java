package com.casic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MyCounter {
    Configuration conf=null;
    Connection conn=null;
    Table table=null;
    @Before
    public void befor() {
        conf = HBaseConfiguration.create();
        try {
            conn= ConnectionFactory.createConnection(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试单个计数器
     * @throws Exception
     */
    @Test
    public void getOneCounter() throws  Exception{
        TableName name = TableName.valueOf("ns1:t6");
         table = conn.getTable(name);
        table.incrementColumnValue(Bytes.toBytes("row1"),Bytes.toBytes("f1"),
                Bytes.toBytes("month"),1);
    }


    /**
     * 测试多个计数器
     * @throws Exception
     */
    @Test
    public void getMoreCounter() throws  Exception{
        TableName name = TableName.valueOf("ns1:t6");
        table = conn.getTable(name);
        Increment increment = new Increment(Bytes.toBytes("row1"));
        increment.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("month"),10);
        table.increment(increment);
    }
    @After
    public void after(){
        try {
            table.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
