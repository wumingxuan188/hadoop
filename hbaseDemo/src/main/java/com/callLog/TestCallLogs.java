package com.callLog;

import com.teacher.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;

/**
 * 测试rowkey的设计
 */
public class TestCallLogs {
    Configuration conf = null;
    Connection conn = null;
    Table table = null;
   @Before
    public void befor(){
        conf = HBaseConfiguration.create();
        try {
            conn= ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void put() throws Exception {
        /**
         * rowKey设计规则
         * regionNo ,主叫,时间,方向,被叫,时长
         */
        SimpleDateFormat sf = new SimpleDateFormat("yyyyMMddHHmmss");
        String time = sf.format(new Date());
        String time1 = time.substring(0, 6);
        String callerId = "13734452463";
        String calleeIid = "15711144188";
        //格式化区域用
        DecimalFormat df = new DecimalFormat("00");
        //格式化时长用
        DecimalFormat dff = new DecimalFormat("00000");
        String talkTime = dff.format(100);
        int i = (calleeIid + time).hashCode();
        //先与运算后再取模,防止有负数出现
        int hash = (i & Integer.MAX_VALUE) % 100;
        //格式化区域号
        String regNo = df.format(hash);
        String rowkey = regNo + "," + callerId + "," + time1 + ",0," + calleeIid + "," + talkTime;
        TableName name = TableName.valueOf("ns1:callLogs");
        table = conn.getTable(name);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("callerPos"), Bytes.toBytes("河北"));
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("calleePos"), Bytes.toBytes("河南"));
        table.put(put);

    }

    // @Test
    public void test2(String callerId1, String calleeId1) throws Exception {
        TableName tname = TableName.valueOf("ns1:callLogs");
        table = conn.getTable(tname);

        /*String callerId = "13734452463" ;
        String calleeId = "15711144188" ;*/
        String callerId = callerId1;
        String calleeId = calleeId1;
        SimpleDateFormat sdf = new SimpleDateFormat();
        sdf.applyPattern("yyyyMMddHHmmss");
        String callTime = sdf.format(new Date());
        int duration = 100;
        DecimalFormat dff = new DecimalFormat();
        dff.applyPattern("00000");
        String durStr = dff.format(duration);

        //区域00-99
        int hash = (callerId + callTime.substring(0, 6)).hashCode();
        hash = (hash & Integer.MAX_VALUE) % 100;

        //hash区域号
        DecimalFormat df = new DecimalFormat();
        df.applyPattern("00");
        String regNo = df.format(hash);

        //拼接rowkey
        //xx , callerid , time ,  direction, calleid  ,duration
        String rowkey = regNo + "," + callerId + "," + callTime + "," + "0," + calleeId + "," + durStr;
        byte[] rowid = Bytes.toBytes(rowkey);
        Put put = new Put(rowid);
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("callerPos"), Bytes.toBytes("河北"));
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("calleePos"), Bytes.toBytes("河南"));
        //执行插入
        table.put(put);
        System.out.println("over");
    }

    @Test
    public void test3() throws Exception {
        String[] arr = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"};
        Random random = new Random();
       // String cid = "13";
//        String cid1 = "15";


        for (int j=0;j < 10;j ++) {
            String cid = "13";
            String cid1 = "15";
            for (Integer i = 0; i < 9; i++) {
                int i1 = random.nextInt(10);
                cid = cid + arr[i1];
                cid1 = cid1 + arr[i1];

            }
            test2(cid,cid1);
            Thread.sleep(100);
        }
        //System.err.println(cid);
        //test2();
    }

    @Test
    public void get() throws Exception {
        TableName tname = TableName.valueOf("ns1:callLogs");
        table = conn.getTable(tname);
        Scan scan = new Scan();
        //13994688208
        //String phone="13994688208";
        String regNo = Util.getRegNo("13994688208", "201901");
        scan.setStartRow(Bytes.toBytes(regNo+","+"13994688208"+","+"201901"));
        scan.setStopRow(Bytes.toBytes(regNo+","+"13994688208"+","+"201902"));
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()){
            Result re = iterator.next();
            byte[] row = re.getRow();
            System.err.println(Bytes.toString(row));
        }

    }
  @After
    public void after(){
        try {
            table.close();
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
