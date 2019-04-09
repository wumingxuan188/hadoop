package com.casic.zookpeer;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.util.List;

public class TestZk {

    /**
     * 测试查看路径
     */
    @Test
    public void testSee() throws  Exception{
        ZooKeeper zkCli = new ZooKeeper("192.168.116.201",5000,null);
        List<String> children = zkCli.getChildren("/", null);
        for (String child : children) {
            System.err.println(child);
        }
    }

    /**
     * 测试查看所有节点
     * @throws Exception
     */
    @Test
    public  void lsAll()throws Exception{
        ls("/a");
    }
    /**
     * 递归查看子节点
     */
    public void ls(String path) throws  Exception{
        System.err.println(path);
        ZooKeeper zkCli = new ZooKeeper("192.168.116.201",5000,null);
        List<String> children = zkCli.getChildren(path, null);
        if (children==null||children.isEmpty()){
            return;
        }
        for (String child : children) {
            if(path.equals("/")){
                ls(path+child);
            }else {
                ls(path+"/"+child);
            }
        }
    }

    /**
     * 创建路径 和数据
     * @throws Exception
     */
    @Test
    public void testCreate() throws  Exception{
        ZooKeeper zkCli = new ZooKeeper("192.168.116.201",5000,null);
        String s = zkCli.create("/c", "toms".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * 从指定的路径下获取数据
     * @throws Exception
     */
    @Test
    public void getData() throws  Exception{
        Stat st=new Stat();
        ZooKeeper zkCli = new ZooKeeper("192.168.116.201",5000,null);
        byte[] data = zkCli.getData("/c", null, st);
        System.out.println(new String(data));
        System.out.println(st.getCtime()+"  "+st.getAversion());

    }

    /**
     * 修改数据,修改数据时版本要对应,-1表示最新版本
     * @throws Exception
     */
    @Test
    public void setData() throws  Exception{
        Stat st=new Stat();
        ZooKeeper zkCli = new ZooKeeper("192.168.116.201",5000,null);
        //修改数据,版本号与现在的对应,-1表示最新版本
        zkCli.setData("/a","tom".getBytes(),-1);

    }

    /**
     * 删除指定路径和数据,-1表示最新版本,版本要与之对应,否
     * 则删除不了
     * @throws Exception
     */
    @Test
    public void del() throws Exception{
             ZooKeeper zkCli = new ZooKeeper("192.168.116.201",5000,null);
             zkCli.delete("/b",-1);
    }

    /**
     *测试观察器(只能检测一次)
     */
    @Test
    public void testWatch() throws Exception{
        ZooKeeper zkCli = new ZooKeeper("192.168.116.201",5000,null);
        zkCli.getData("/a", new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("数据发生改变");
            }
        },new Stat());
        System.in.read();
    }

    /**
     * 测试检测器(一直检测)
     * new stat 是创建一个状态,把version.什么的都放进去
     * @throws Exception
     */
    @Test
    public void testAllWatch() throws Exception{
        final ZooKeeper zkCli = new ZooKeeper("192.168.116.201",5000,null);
       final Watcher watcher=new Watcher() {
           public void process(WatchedEvent watchedEvent) {
               System.err.println("数据发生改变");
               try {
                   zkCli.getData("/a",this,new Stat());
               } catch (KeeperException e) {
                   e.printStackTrace();
               } catch (InterruptedException e) {
                   e.printStackTrace();
               }
           }
       };
        zkCli.getData("/a",watcher,new Stat());

        System.in.read();
    }
}
