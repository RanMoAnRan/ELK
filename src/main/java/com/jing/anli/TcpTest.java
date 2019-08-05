package com.jing.anli;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * @Date 2019/8/3
 */
public class TcpTest {


    public static void main(String[] args) throws IOException {

        // 向服务器端发送请求，服务器IP地址和服务器监听的端口号
        Socket client = new Socket("node01", 9876);

        // 通过printWriter 来向服务器发送消息
        PrintWriter printWriter = new PrintWriter(client.getOutputStream());
        System.out.println("连接已建立...");
        for(int i=0;i<10;i++){
            // 发送消息
            printWriter.println("hello logstash , 这是第"+i+" 条消息");
            printWriter.flush();
        }

    }

}
