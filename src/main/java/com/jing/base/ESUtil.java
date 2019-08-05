package com.jing.base;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author RanMoAnRan
 * @ClassName: ESUtil
 * @projectName ELK
 * @description: TODO
 * @date 2019/8/3 18:13
 */
public class ESUtil {
    private static TransportClient client;

    public static TransportClient createTransportClient() {
        try {
            Settings myes = Settings
                    .builder()
                    .put("cluster.name", "myes")
                    //自动发现我们其他的es的服务器(下面的addTransportAddress可以只写一个主机)
                    //.put("client.transport.sniff", "true")
                    .build();
            client = new PreBuiltTransportClient(myes)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("hadoop01"), 9300))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("hadoop02"), 9300))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("hadoop03"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;
    }
}
