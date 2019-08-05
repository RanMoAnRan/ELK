package com.jing.anli.excel;

import com.alibaba.fastjson.JSON;


import com.jing.anli.Article;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;


public class EsUtil {


    /**
     * 获取客户端连接对象
     */
    public static TransportClient createClient(){
        TransportClient client = null;
        Settings myes = Settings.builder().put("cluster.name", "myes").build();
        try {
            client = new PreBuiltTransportClient(myes)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("node01"), 9300))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("node02"), 9300))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("node03"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;
    }

    /**
     * 插入数据
     */
    public static void bathPutData(List<Article> list){

        TransportClient client = createClient();
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for (Article article : list) {

            IndexRequestBuilder indexRequestBuilder = client.prepareIndex("articles", "article", article.getId())
                    .setSource(JSON.toJSONString(article), XContentType.JSON);
            bulkRequestBuilder.add(indexRequestBuilder);
        }

        bulkRequestBuilder.get();
        client.close();
    }


    /**
     * 词条查询
     */
    public static List<Article> termQurey(String title){


        TransportClient client = createClient();
        SearchResponse searchResponse = client.prepareSearch("articles")
                .setTypes("article")
                .setQuery(QueryBuilders.termQuery("title", title))
                .get();

        SearchHit[] hits = searchResponse.getHits().getHits();
        ArrayList<Article> articles = new ArrayList<>();
        for (SearchHit hit : hits) {

            String sourceAsString = hit.getSourceAsString();
            Article article = JSON.parseObject(sourceAsString, Article.class);
            articles.add(article);
        }
        client.close();
        return articles;
    }
}
