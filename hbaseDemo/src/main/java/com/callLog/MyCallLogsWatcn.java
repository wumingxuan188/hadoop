package com.callLog;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
//com.callLog.MyCallLogsWatcn
public class MyCallLogsWatcn extends BaseRegionObserver {

    /**
     * 在主叫插入之前
     * 再插入一条被叫的信息
     * @param e
     * @param put
     * @param edit
     * @param durability
     * @throws IOException
     */
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.postPut(e, put, edit, durability);
        FileWriter fw = new FileWriter("/home/centos/kkk.txt",true);
       // fw.write("enter"+ "\r\n");
        DecimalFormat df=new DecimalFormat("00");
        //SimpleDateFormat sf=new SimpleDateFormat("yyyyMMddHHmmss");
        String tableName = TableName.valueOf("ns1:callLogs").getNameAsString();
        //获取每次操作表名称
        String name = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
        //fw.write("enter1"+ "\r\n");
       // fw.write(name);
        if(!tableName.equals(name)) {
            return;
        }
        String rowKey = Bytes.toString(put.getRow());
        String[] arr = rowKey.split(",");
        fw.write(arr+ "\r\n");
        if("1".equals(arr[3])) {
            return;
        }
        //regionNo ,主叫,时间,方向,被叫,时长
        int hash = (arr[4] + arr[2]).hashCode();
        hash = (hash & Integer.MAX_VALUE) % 100;
        String newHash = df.format(hash);
        String newKey = newHash + "," + arr[4] + "," + arr[2] + "," + ",1," + arr[1] + "," + arr[5];
        fw.write(newKey+ "\r\n");
        Put newPut = new Put(Bytes.toBytes(newKey));
        newPut.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("test"),Bytes.toBytes("no"));
        Table table = e.getEnvironment().getTable(TableName.valueOf("ns1:callLogs"));
        fw.write(table.getName().getNameAsString());
        table.put(newPut);
        fw.close();
    }
}
