package com;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;
import tutorialspoint.Emp;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public class TestAvro {
    /**
     * 写入文件,把数据串行化到磁盘中
     *
     * @throws Exception
     */
    @Test
    public void testWrite() throws Exception {
        //创捷写入对象
        SpecificDatumWriter empDatumWriter = new SpecificDatumWriter<Emp>(Emp.class);
        //写入文件
        DataFileWriter<Emp> empFileWriter = new DataFileWriter<Emp>(empDatumWriter);
        //创建对象
        Emp emp = new Emp();
        emp.setAddress("北京视为");
        emp.setAge(11);
        emp.setId(1);
        emp.setName("张无忌");
        emp.setSalary(2);
        //串行化到磁盘
        empFileWriter.create(emp.getSchema(), new File("D:\\avro\\emp.data"));
        empFileWriter.append(emp);
        empFileWriter.append(emp);
        empFileWriter.close();
    }

    /**
     * 读取文件 从磁盘中读取文件
     *
     * @throws Exception
     */
    @Test
    public void testRead() throws Exception {
        //创捷读取对象
        SpecificDatumReader empDatumReader = new SpecificDatumReader<Emp>(Emp.class);
        //读取文件
        DataFileReader<Emp> empFileReader = new DataFileReader<Emp>(new File("D:\\avro\\emp.data"), empDatumReader);
        Iterator<Emp> it = empFileReader.iterator();
        while (it.hasNext()) {
            System.err.println(it.next());
        }
        empFileReader.close();
    }

    /**
     * 直接使用schema文件进行写,不需要编译
     *
     * @throws Exception
     */
    @Test
    public void testWriteInSce() throws Exception {
        //指定定义的schema文件
        Schema schema = new Schema.Parser().parse(new File("D:\\demo.avsc"));
        //创建GenericRecord,相当于Employee
        GenericRecord e1 = new GenericData.Record(schema);
        //向schema文件定义的fileds字段中赋值,每个字段都要赋值
        e1.put("name", "ramu");
        e1.put("id", 001);
        e1.put("salary", 30000);
        e1.put("age", 25);
       e1.put("address", "北京市委");
        //创建writer对象
        DatumWriter w1 = new SpecificDatumWriter(schema);
        //写入文件
        DataFileWriter w2 = new DataFileWriter(w1);
        w2.create(schema, new File("D:\\avro\\emp5.data"));
        //向文件中追加内容
        w2.append(e1);
        w2.append(e1);
        w2.append(e1);
        w2.append(e1);
        w2.append(e1);
        w2.close();
    }

    /**
     * 直接用schame文件进行读 不用编译
     * @throws Exception
     */
    @Test
    public void testReadInSce() throws Exception {
        Schema schema = new Schema.Parser().parse(new File("D:\\demo.avsc"));
        GenericRecord e1 = new GenericData.Record(schema);
        DatumReader r1 = new SpecificDatumReader (schema);
        DataFileReader r2 = new DataFileReader(new File("D:\\avro\\emp5.data"),r1);
        while(r2.hasNext()){
            GenericRecord rec = (GenericRecord)r2.next();
            System.out.println(rec.get("name"));
            System.out.println(rec.get("id"));
            System.out.println(rec.get("age"));
            System.out.println(rec.get("address"));
        }
        r2.close();
    }
}
