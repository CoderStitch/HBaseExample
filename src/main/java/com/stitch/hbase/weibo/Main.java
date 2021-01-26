package com.stitch.hbase.weibo;

import java.util.List;

public class Main {

    /**
     * 发布微博内容
     * 添加关注
     * 取消关注
     * 展示内容
     */
    public static void testPublishContent(WeiBo wb){
        wb.publishContent("0001", "今天买了一包空气，送了点薯片，非常开心！！");
        wb.publishContent("0001", "今天天气不错。");
    }
    public static void testAddAttend(WeiBo wb){
        wb.publishContent("0008", "准备下课！");
        wb.publishContent("0009", "准备关机！");
        wb.addAttends("0001", "0008", "0009");
    }
    public static void testRemoveAttend(WeiBo wb){
        wb.removeAttends("0001", "0008");
    }
    public static void testShowMessage(WeiBo wb){
        List<Message> messages = wb.getAttendsContent("0001");
        for(Message message : messages){
            System.out.println(message);
        }
    }
    public static void main(String[] args) {
        WeiBo weibo = new WeiBo();
        weibo.initTable();
        testPublishContent(weibo);
        testAddAttend(weibo);
        testShowMessage(weibo);
        testRemoveAttend(weibo);
        testShowMessage(weibo);
    }

}
