package com.jing.anli.excel;

import com.jing.anli.Article;

import java.util.List;

/**
 * @Date 2019/8/3
 */
public class AppTest {

    public static void main(String[] args) throws Exception {

        //1.写入hbase和es
        //writeData();



        //2.查询hbase和es
        queryData("奇瑞");



    }

    /**
     * 查询数据
     * @param titleName
     * @return
     */
    private static void queryData(String titleName) {

        //1.先查询es，获取索引ID
        List<Article> articles = EsUtil.termQurey(titleName);

        //2.根据索引ID（rowkey）,查询hbase
        for (Article article : articles) {
            String content = HbaseUtil.queryByRowkey("articles", "article", "content", article.getId());
            System.out.println(content);
        }
    }

    private static void writeData() throws Exception {
        String path = "B:\\video\\elk\\0802\\elk-day02\\hbaseEs.xlsx";
        List<Article> articles = ExcelUtil.parseExcel(path);
        //写入hbase
        HbaseUtil.batchData(articles);

        //写入es
        EsUtil.bathPutData(articles);
    }
}
