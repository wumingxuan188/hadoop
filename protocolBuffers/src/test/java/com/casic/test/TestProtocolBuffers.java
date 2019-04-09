package com.casic.test;

import com.example.tutorial.AddressBookProtos;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;

public class TestProtocolBuffers {
    /**
     * 利用protoc 串行化写入文件中
     * (串行化)
     * @throws Exception
     */
    @Test
    public void testWrite() throws Exception {
        AddressBookProtos.Person person = AddressBookProtos.Person.newBuilder().setEmail("101010@qq.com").setId(1).setName("wangwu").
                addPhone(AddressBookProtos.Person.PhoneNumber.newBuilder()
                        .setNumber("+351 999 999 999")
                        .setType(AddressBookProtos.Person.PhoneType.HOME)
                        .build()).build();
        FileOutputStream fos=new FileOutputStream("D:\\protoc\\output\\protoc.data");
        person.writeTo(fos);
        fos.close();
    }

    /**
     * 反串行化
     * @throws Exception
     */
    @Test
    public void testRead() throws Exception {

        FileInputStream fis=new FileInputStream("D:\\protoc\\output\\protoc.data");
        AddressBookProtos.Person person = AddressBookProtos.Person.parseFrom(fis);
        fis.close();
        System.err.println(person);
    }

}
