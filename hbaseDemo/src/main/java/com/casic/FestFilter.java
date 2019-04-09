package com.casic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Iterator;

public class FestFilter {

    /**
     * 行过滤器,过滤rowkey的
     *
     * @throws Exception
     */
    @Test
    public void testRowFilter() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t1");
        //设置过滤器,new BinaryComparator二进制对比器
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("row0100")));
        Scan scan = new Scan();
        scan.setFilter(rowFilter);
        Table table = conn.getTable(tableName);
        ResultScanner results = table.getScanner(scan);
        Iterator<Result> it = results.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            //获取rowKey
            byte[] row = r.getRow();
            //
            System.err.println(Bytes.toString(row));
        }
        table.close();
        conn.close();
    }

    /**
     * 测试FamilyFilter过滤器,
     * 过滤列族的
     *
     * @throws Exception
     */
    @Test
    public void testFamilyFilter() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t2");
        //列族过滤器
        FamilyFilter filter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("f1")));
        Scan scan = new Scan();
        //向scan中添加过滤器
        scan.setFilter(filter);
        Table table = conn.getTable(tableName);
        ResultScanner results = table.getScanner(scan);
        Iterator<Result> it = results.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            byte[] f1id = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f2id = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("id"));
            byte[] f1name = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            byte[] f2name = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));
            System.err.println("f1Id:" + Bytes.toString(f1id) + ",f2Id:" + Bytes.toString(f2id) + ",f1name:" + Bytes.toString(f1name) + ",f2name:" + Bytes.toString(f2name));
        }
        table.close();
        conn.close();
    }

    /**
     * 测试列过滤器QualifierFilter
     * 选出与条件相同列的值
     *
     * @throws Exception
     */
    @Test
    public void testQuliFilter() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t2");
        //列过滤器
        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes("id")));
        Scan scan = new Scan();
        //向scan中添加过滤器
        scan.setFilter(qualifierFilter);
        Table table = conn.getTable(tableName);
        ResultScanner results = table.getScanner(scan);
        Iterator<Result> it = results.iterator();
        while (it.hasNext()) {

            Result r = it.next();
            byte[] f1id = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f2id = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("id"));
            byte[] f1name = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            byte[] f2name = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));
            System.err.println("f1Id:" + Bytes.toString(f1id) + ",f2Id:" + Bytes.toString(f2id) + ",f1name:" + Bytes.toString(f1name) + ",f2name:" + Bytes.toString(f2name));
        }
        table.close();
        conn.close();
    }

    /**
     * 测试值过滤器valueFilter
     * 过滤掉不符合条件的值
     * 如果不符合,则整行都过滤掉
     *
     * @throws Exception
     */
    @Test
    public void testValueFilter() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t2");
        //值过滤器
        //new SubstringComparator匹配值中是否又此字符串
        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("3.1"));
        Scan scan = new Scan();
        //向scan中添加过滤器
        scan.setFilter(valueFilter);
        Table table = conn.getTable(tableName);
        ResultScanner results = table.getScanner(scan);
        Iterator<Result> it = results.iterator();
        while (it.hasNext()) {

            Result r = it.next();
            byte[] f1id = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f2id = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("id"));
            byte[] f1name = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            byte[] f2name = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));
            System.err.println("f1Id:" + Bytes.toString(f1id) + ",f2Id:" + Bytes.toString(f2id) + ",f1name:" + Bytes.toString(f1name) + ",f2name:" + Bytes.toString(f2name));
        }
        table.close();
        conn.close();
    }

    /**
     * 测试依赖列过滤器
     *
     * @throws Exception
     */
    @Test
    public void testDepFilter() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t2");
        DependentColumnFilter depFileter = new DependentColumnFilter(Bytes.toBytes("f2"),
                Bytes.toBytes("name"), false, CompareFilter.CompareOp.EQUAL,
                new SubstringComparator(".2"));
        Scan scan = new Scan();
        //向scan中添加过滤器
        scan.setFilter(depFileter);
        Table table = conn.getTable(tableName);
        ResultScanner results = table.getScanner(scan);
        Iterator<Result> it = results.iterator();
        while (it.hasNext()) {

            Result r = it.next();
            byte[] f1id = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f2id = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("id"));
            byte[] f1name = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            byte[] f2name = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));
            System.err.println("f1Id:" + Bytes.toString(f1id) + ",f2Id:" + Bytes.toString(f2id) + ",f1name:" + Bytes.toString(f1name) + ",f2name:" + Bytes.toString(f2name));
        }
        table.close();
        conn.close();
    }

    /**
     * 单列值排除过滤器,去掉过滤使用的列,对列的值进行过滤
     * 不返回过滤条件使用的列的值
     * @throws Exception
     */
    @Test
    public void testQuliferFilter() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t2");
        SingleColumnValueExcludeFilter svx=new SingleColumnValueExcludeFilter(Bytes.toBytes("f2"),
                Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes("tom2.2")));
        Scan scan = new Scan();
        //向scan中添加过滤器
        scan.setFilter(svx);
        Table table = conn.getTable(tableName);
        ResultScanner results = table.getScanner(scan);
        Iterator<Result> it = results.iterator();
        while (it.hasNext()) {

            Result r = it.next();
            byte[] f1id = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f2id = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("id"));
            byte[] f1name = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            byte[] f2name = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));
            System.err.println("f1Id:" + Bytes.toString(f1id) + ",f2Id:" + Bytes.toString(f2id) + ",f1name:" + Bytes.toString(f1name) + ",f2name:" + Bytes.toString(f2name));
        }
        table.close();
        conn.close();
    }

    /**
     * rowkey 前缀过滤器
     * 对rowkey进行过滤
     * @throws Exception
     */
    @Test
    public void testprefixFilter() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t1");
        //rowkey前缀过滤器
        PrefixFilter pff=new PrefixFilter(Bytes.toBytes("row222"));
        Scan scan = new Scan();
        //向scan中添加过滤器
        scan.setFilter(pff);
        Table table = conn.getTable(tableName);
        ResultScanner results = table.getScanner(scan);
        Iterator<Result> it = results.iterator();
        while (it.hasNext()) {

            Result r = it.next();
            byte[] f1id = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            //byte[] f2id = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("id"));
            byte[] f1name = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            //byte[] f2name = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));
            System.err.println("f1Id:" + Bytes.toString(f1id) +",f1name:" + Bytes.toString(f1name) );
        }
        table.close();
        conn.close();
    }


    /**
     * 分页过滤器
     * @throws Exception
     */
    @Test
    public void testPageFilter() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t1");
        //分页过滤器
        PageFilter pageFilter=new PageFilter(10);
        Scan scan = new Scan();
        //向scan中添加过滤器
        scan.setFilter(pageFilter);
        Table table = conn.getTable(tableName);
        ResultScanner results = table.getScanner(scan);
        Iterator<Result> it = results.iterator();
        while (it.hasNext()) {

            Result r = it.next();
            byte[] f1id = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            //byte[] f2id = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("id"));
            byte[] f1name = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            //byte[] f2name = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));
            System.err.println("f1Id:" + Bytes.toString(f1id) +",f1name:" + Bytes.toString(f1name) );
        }
        table.close();
        conn.close();
    }

    /**
     * 列分页过滤器
     *new ColumnPaginationFilter(列数量,开始数)
     * @throws Exception
     */
    @Test
    public void testColumnPageFilter() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t2");
        ColumnPaginationFilter paginationFilter=new ColumnPaginationFilter(2,2);
        Scan scan = new Scan();
        //向scan中添加过滤器
        scan.setFilter(paginationFilter);
        Table table = conn.getTable(tableName);
        ResultScanner results = table.getScanner(scan);
        Iterator<Result> it = results.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            byte[] f1id = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f2id = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("id"));
            byte[] f1name = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            byte[] f2name = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));
            System.err.println("f1Id:" + Bytes.toString(f1id) + ",f2Id:" + Bytes.toString(f2id) + ",f1name:" + Bytes.toString(f1name) + ",f2name:" + Bytes.toString(f2name));
        }
        table.close();
        conn.close();

    }




    /**
     *  单列值过滤器
     *  显示符合条件整行的数据
     * @throws Exception
     */
    @Test
    public void testSingleFilter() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t2");
        //单列值过滤器
        //RegexStringComparator  正则对比器
        SingleColumnValueFilter singleColumnValueFilter=new SingleColumnValueFilter(Bytes.toBytes("f2"),
                Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL,new RegexStringComparator("3.2$"));
        Scan scan = new Scan();
        //向scan中添加过滤器
        scan.setFilter(singleColumnValueFilter);
        Table table = conn.getTable(tableName);
        ResultScanner results = table.getScanner(scan);
        Iterator<Result> it = results.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            byte[] row = r.getRow();
            byte[] f1id = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f2id = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("id"));
            byte[] f1name = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            byte[] f2name = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));
            System.err.println("rowKey:"+Bytes.toString(row)+",f1Id:"
                    + Bytes.toString(f1id) + ",f2Id:" +
                    Bytes.toString(f2id) + ",f1name:" + Bytes.toString(f1name) +
                    ",f2name:" + Bytes.toString(f2name));
        }
        table.close();
        conn.close();
    }


    /**
     * 测试过滤器集合
     * select * from t7 where ((age <= 13) and (name like '%t')
     * or
     * (age > 13) and (name like 't%'))
     * @throws Exception
     */
    @Test
    public void testListFilter() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t2");
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        SingleColumnValueFilter rage=new SingleColumnValueFilter(Bytes.toBytes("f2"),
                Bytes.toBytes("age"), CompareFilter.CompareOp.LESS_OR_EQUAL,new BinaryComparator(Bytes.toBytes("13")));
        SingleColumnValueFilter rname=new SingleColumnValueFilter(Bytes.toBytes("f2"),
                Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL,new RegexStringComparator("t$"));
        //创建过滤器集合
        FilterList fa=new FilterList(FilterList.Operator.MUST_PASS_ALL);
        //向过滤器中添加单个过滤器
        fa.addFilter(rage);
        fa.addFilter(rname);
        SingleColumnValueFilter lage=new SingleColumnValueFilter(Bytes.toBytes("f2"),
                Bytes.toBytes("age"), CompareFilter.CompareOp.GREATER,new BinaryComparator(Bytes.toBytes("13")));
        SingleColumnValueFilter lname=new SingleColumnValueFilter(Bytes.toBytes("f2"),
                Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL,new RegexStringComparator("^t"));
       //设置必须满足lage,lage所有的条件
        FilterList fb =new FilterList(FilterList.Operator.MUST_PASS_ALL);
       fb.addFilter(lage);
        fb.addFilter(lage);

        FilterList all =new FilterList(FilterList.Operator.MUST_PASS_ONE);
        all.addFilter(fa);
        all.addFilter(fb);
       scan.setFilter(all);
        ResultScanner results = table.getScanner(scan);
        Iterator<Result> it = results.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            byte[] row = r.getRow();
            byte[] f1age = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age"));
            byte[] f2age = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("age"));
            byte[] f1name = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            byte[] f2name = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));
            System.err.println("rowKey:"+Bytes.toString(row)+",f1age:"
                    + Bytes.toString(f1age) + ",f2age:" +
                    Bytes.toString(f2age) + ",f1name:" + Bytes.toString(f1name) +
                    ",f2name:" + Bytes.toString(f2name));
        }
        table.close();
        conn.close();
    }


    @Test
    public void testHash(){
        int i = "15711144188_20190301".hashCode();
        System.err.println(i);
        int i1 = i % 100;
        System.out.println(i1);
    }

}
