package com.casic.util;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class Util {
    //获取主机名
    public static String getName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    return  null;
    }
    //获取进程名称
    public static String getPid(){
        return ManagementFactory.getRuntimeMXBean().getName();
    }
    //获取线程名称
    public static  String getTid(){
        return  Thread.currentThread().getName();
    }
    //获取对象信息
    public static String getOid(Object object){
        String name = object.getClass().getName();
        int hashCode = name.hashCode();
        return  name +"@"+hashCode;
    }
    //获取信息
    public  static String getInfo(Object obj,String msg){
      return   getName() + "," + getPid() + "," + getTid() + "," + getOid(obj) + "," + msg ;
    }
    //发送信息
    public  static  void sendInfo(Object obj,String msg){
        Socket socket= null;
        try {
            socket = new Socket("127.0.0.1",8888);
            OutputStream stream = socket.getOutputStream();
            String info = getInfo(obj, msg);
            stream.write((info+ "\r\n").getBytes());
            stream.flush();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
