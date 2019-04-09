package com.casic.test;

import com.casic.util.Util;

public class TestThread {
    public static void main(String[] args) {
        Thread t = new Thread() {
            @Override
            public void run() {
                String name = Util.getName();
                String tid = Util.getTid();
                System.err.println(name+","+tid);
            }
        };
        //设置守护线程
      //  t.setDaemon(true);
        t.start();
    }
}




