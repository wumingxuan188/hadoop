package com.casic;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;

/**
 * 区域观察器,
 * 检测区域的动作
 * 继承BaseRegionObserver
 * 重写感兴趣的动作
 */
public class MyReginServer extends BaseRegionObserver {
   //创建一个写入文档方法
    private void outInfo(String str){
        try {
            FileWriter fw = new FileWriter("/home/centos/coprocessor.txt",true);
            fw.write(str + "\r\n");
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    //启动时观察
    public void start(CoprocessorEnvironment e) throws IOException {
        super.start(e);
        outInfo("MyReginServer.start");

    }
    //停止habse时监测
    public void stop(CoprocessorEnvironment e) throws IOException {
        super.stop(e);
        outInfo("MyReginServer.stop");
    }
    //查询数据之前监测
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        super.preGetOp(e, get, results);
        outInfo("MyReginServer.preGetOp"+","+ Bytes.toString(get.getRow()));
    }
    //查询数据之后监测
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        super.postGetOp(e, get, results);
        outInfo("MyReginServer.postGetOp"+","+Bytes.toString(get.getRow()));
    }
    //插入数据之前监测
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.prePut(e, put, edit, durability);
        outInfo("MyReginServer.prePut"+","+Bytes.toString(put.getRow()));
    }
    //插入数据之前监测
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.postPut(e, put, edit, durability);
        outInfo("MyReginServer.postPut"+","+Bytes.toString(put.getRow()));
    }
}
