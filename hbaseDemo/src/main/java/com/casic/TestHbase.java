package com.casic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

public class TestHbase {

    /**
     * 向hbase中增加数据
     *
     * @throws Exception
     */
    @Test
    public void put() throws Exception {
        //创建conf对象
        Configuration conf = HBaseConfiguration.create();
        //创建连接工厂
        Connection connection = ConnectionFactory.createConnection(conf);
        //设置表名
        TableName tableName = TableName.valueOf("ns1:t1");
        //根据表明获得表
        Table table = connection.getTable(tableName);
        //利用hbase中的类库把字符串转成字符数组
        byte[] row = Bytes.toBytes("row5");
        //创建一个put对象(存放rowkey和数据)
        Put put = new Put(row);
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("id"), Bytes.toBytes(6));
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("wangwu1"));
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age"), Bytes.toBytes(18));
        //用table的put的方法放到hbase中
        table.put(put);
    }

    /**
     * 通过get获取数据
     *
     * @throws Exception
     */
    @Test
    public void get() throws Exception {
        //创建conf对象
        Configuration conf = HBaseConfiguration.create();
        //创建连接工厂
        Connection factory = ConnectionFactory.createConnection(conf);
        //获取表名
        TableName tableName = TableName.valueOf("ns1:t1");
        //通过表明获取table对象
        Table table = factory.getTable(tableName);
        //创建get对象,传入rowkey
        Get get = new Get(Bytes.toBytes("row5"));
        //设置版本数
        get.setMaxVersions();
        //执行get方法(获取当行结果)
        Result result = table.get(get);
        //获取当行指定列族,指定列的之
        byte[] id = result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));

        byte[] name = result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
        //Bytes工具进行转换为适当类型
        System.err.println(Bytes.toInt(id));
        System.err.println(Bytes.toString(name));
    }


    /**
     * 批量提交(写入hbase中)
     *
     * @throws Exception
     */
    @Test
    public void bigInsert() throws Exception {
        DecimalFormat df = new DecimalFormat("0000");
        Configuration conf = HBaseConfiguration.create();
        Connection factory = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t3");
        HTable table = (HTable) factory.getTable(tableName);
        //关闭自动提交
        table.setAutoFlush(false);

        for (int i = 0; i < 10000; i++) {
            Put put = new Put(Bytes.toBytes("row" + df.format(i)));
            //关闭写前日志(提高写入效率,不提倡,关闭后如果出错,便无法恢复数据)
            put.setWriteToWAL(false);
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("id"), Bytes.toBytes(i));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("tom" + i));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age"), Bytes.toBytes(i % 100));
            table.put(put);
            if (i % 2000 == 0) {
                //提交
                table.flushCommits();
            }
        }
        table.flushCommits();
    }


    /**
     * 创建名称空间
     *
     * @throws Exception
     */
    @Test
    public void createNamespace() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection con = ConnectionFactory.createConnection(conf);
        //通过连接获取一个管理员操作符
        Admin admin = con.getAdmin();
        //创建名称描述符
        NamespaceDescriptor nsd = NamespaceDescriptor.create("ns2").build();
        //执行创建
        admin.createNamespace(nsd);
        //关闭
        admin.close();
        //关闭连接
        con.close();
    }

    /**
     * 获取名称空间
     *
     * @throws Exception
     */
    @Test
    public void list_namespace() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection con = ConnectionFactory.createConnection(conf);
        Admin admin = con.getAdmin();
        //获取名称空间描述符
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor nsd : namespaceDescriptors) {
            System.err.println(nsd.getName());
        }
    }

    /**
     * 创建表
     *
     * @throws Exception
     */
    @Test
    public void createTable() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection con = ConnectionFactory.createConnection(conf);
        Admin admin = con.getAdmin();
        TableName name = TableName.valueOf("ns2:t2");
        //创建table描述符
        HTableDescriptor htd = new HTableDescriptor(name);

        //创建列族描述符
        HColumnDescriptor colum = new HColumnDescriptor("f1");
        HColumnDescriptor colum2 = new HColumnDescriptor("f2");
        //设置删除后保存
        colum.setKeepDeletedCells(true);
        //设置生存时间(在列级别中)
        colum.setTimeToLive(2000);
        //向table描述符中添加列族描述符
        htd.addFamily(colum);
        htd.addFamily(colum2);
        //创建表
        admin.createTable(htd);
        admin.close();
        con.close();
    }


    /**
     * 删除表,先禁用表
     *
     * @throws Exception
     */
    @Test
    public void deleteTable() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection con = ConnectionFactory.createConnection(conf);
        Admin admin = con.getAdmin();
        //表名
        TableName name = TableName.valueOf("ns2:t2");
        //先禁用表
        admin.disableTable(name);
        //删除表
        admin.deleteTable(name);

        admin.close();
        con.close();
    }


    /**
     * 根据条件删除表中的数据
     *
     * @throws Exception
     */
    @Test
    public void delteData() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        //创建一个表名
        TableName tableName = TableName.valueOf("ns1:t1");
        Table table = conn.getTable(tableName);
        //创建删除对象 传入rowkey
        Delete delete = new Delete(Bytes.toBytes("row0001"));
        //向删除对象中添加列族中的列
        delete.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"));
        delete.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age"));
        //根据条件进行删除
        table.delete(delete);
        table.close();
        conn.close();
    }


    /**
     * scan扫描表(全表扫描)
     *
     * @throws Exception
     */
    @Test
    public void scan() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        //创建一个表名
        TableName tableName = TableName.valueOf("ns1:t1");
        Table table = conn.getTable(tableName);
       //创建scan对象,空参表示全表扫描
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
           //获取指定列族和指定行的数据
            byte[] value = result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            System.err.println(Bytes.toString(value));
        }
    }

    /**
     * 从指定位置扫描
     *
     * @throws Exception
     */
    @Test
    public void scan1() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        //创建一个表名
        TableName tableName = TableName.valueOf("ns1:t1");
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        //设置开始扫描位置
        scan.setStartRow(Bytes.toBytes("row0005"));
        //设置结束扫描位置
        scan.setStopRow(Bytes.toBytes("row8888"));
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            byte[] value = result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            System.err.println(Bytes.toString(value));
        }
    }


    /**
     * 动态解析结果
     *
     * @throws Exception
     */
    @Test
    public void scan3() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t1");
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("row0005"));
        scan.setStopRow(Bytes.toBytes("row8888"));
        //得到结果
        ResultScanner rs = table.getScanner(scan);
        //得到结果的迭代器
        Iterator<Result> it = rs.iterator();
        Integer id = 0;
        String name = null;
        Integer age = 0;
        while (it.hasNext()) {
            //获取行值
            Result next = it.next();
            //获取指定列族
            Map<byte[], byte[]> f1 = next.getFamilyMap(Bytes.toBytes("f1"));
            //用entry遍历map
            for (Map.Entry<byte[], byte[]> data : f1.entrySet()) {
                byte[] col = data.getKey();
                String column = Bytes.toString(col);
                byte[] value = data.getValue();

                if (column.equals("id")) {
                    id = Bytes.toInt(value);
                } else if (column.equals("name")) {
                    name = Bytes.toString(value);
                } else {
                    age = Bytes.toInt(value);
                }

                System.err.println("id:" + id + ",name:" + name + ",age:" + age);
            }
        }
        table.close();
        conn.close();
    }

    /**
     * 动态解析结果
     *
     * @throws Exception
     */
    @Test
    public void scan4() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t1");
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("row0005"));
        scan.setStopRow(Bytes.toBytes("row8888"));
        //得到结果
        ResultScanner rs = table.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()){
            Result re = it.next();
            Map<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = re.getMap();
                //包括的信息依次为列族,列,时间戳,值
                for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : map.entrySet()) {
                    //获得列族
                    byte[] fam = entry.getKey();
                    String family = Bytes.toString(fam);
                    //System.err.println("列族"+family);
                    NavigableMap<byte[], NavigableMap<Long, byte[]>> value1 = entry.getValue();
                    for (Map.Entry<byte[], NavigableMap<Long, byte[]>> entry2 : value1.entrySet()) {
                    //获得列
                    byte[] row = entry2.getKey();
                    String column = Bytes.toString(row);
                    //System.err.println("列"+column);
                    NavigableMap<Long, byte[]> value2 = entry2.getValue();
                    for (Map.Entry<Long, byte[]> data : value2.entrySet()) {
                        //获得时间戳
                        Long time = data.getKey();
                       // System.err.println("时间戳"+time);
                        //获得值
                        byte[] value3 = data.getValue();
                        String value = Bytes.toString(value3);
                        System.err.print(family+","+column+","+time+","+value);
                    }
                }
            }
            System.err.println();
        }
        table.close();
        conn.close();
    }
    @Test
    public void getCaching() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        //创建一个表名
        TableName tableName = TableName.valueOf("ns1:t1");
        Table table = conn.getTable(tableName);
        //创建scan对象,空参表示全表扫描
        Scan scan = new Scan();
        //设置缓存数
        scan.setCaching(1000);
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            //获取指定列族和指定行的数据
            byte[] value = result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            System.err.println(Bytes.toString(value));
        }
    }

    /**
     * 测试批量处理和缓存
     * 缓存是面对行的,设置缓存后  扫描表的速度会快
     * 批量处理是面对列的  ,设置批量处理后,每次next会返回设置的列数
     * @throws Exception
     */
    @Test
    public void testCachAndBatch()throws  Exception{
      Configuration conf = HBaseConfiguration.create();
      Connection conn = ConnectionFactory.createConnection(conf);
      TableName name = TableName.valueOf("ns1:t2");
      Table table = conn.getTable(name);
      Scan scan = new Scan();
      //面行行级的,设置缓存
      scan.setCaching(2);
      //面向列的,设置批处理
      scan.setBatch(2);
      ResultScanner scanner = table.getScanner(scan);
      Iterator<Result> iterator = scanner.iterator();
      while (iterator.hasNext()){
          Result r= iterator.next();
          System.out.println("========================================");
          //得到一行的所有map,key=f1,value=Map<Col,Map<Timestamp,value>>
          NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = r.getMap();
          //
          for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : map.entrySet()) {
              //得到列族
              String f = Bytes.toString(entry.getKey());
              Map<byte[], NavigableMap<Long, byte[]>> colDataMap = entry.getValue();
              for (Map.Entry<byte[], NavigableMap<Long, byte[]>> ets : colDataMap.entrySet()) {
                  String c = Bytes.toString(ets.getKey());
                  Map<Long, byte[]> tsValueMap = ets.getValue();
                  for (Map.Entry<Long, byte[]> e : tsValueMap.entrySet()) {
                      Long ts = e.getKey();
                      String value = Bytes.toString(e.getValue());
                      System.out.print(f + "/" + c + "/" + ts + "=" + value + ",");
                  }
              }
          }
          System.out.println();
      }

  }

}

















