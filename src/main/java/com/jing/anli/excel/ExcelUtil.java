package com.jing.anli.excel;

import com.jing.anli.Article;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @Date 2019/8/3
 * @Autor xxx
 * @Description 表格解析工具类
 */
public class ExcelUtil {

    public static void main(String[] args) throws Exception {

        String path = "B:\\video\\elk\\0802\\elk-day02\\hbaseEs.xlsx";
        List<Article> articles = parseExcel(path);
        System.out.println(articles);
    }


    /**
     * 解析excle
     * 解析表格用的jar：poi
     *
     * @return List<Article>
     */
    public static List<Article> parseExcel(String pathName) throws Exception {


        /**
         * 表格2003 ： .xls   (HSSFWorkbook)
         * 表格2007：  .xlsx  (XSSFWorkbook)
         * csv
         */
        File file = new File(pathName);
        FileInputStream fileInputStream = new FileInputStream(file);
        XSSFWorkbook xssfWorkbook = new XSSFWorkbook(fileInputStream);
        XSSFSheet sheetAt = xssfWorkbook.getSheetAt(0);
        int lastRowNum = sheetAt.getLastRowNum();

        ArrayList<Article> articles = new ArrayList<>();
        int num =0;
        for (int i = 1; i <= lastRowNum; i++) {
            XSSFRow row = sheetAt.getRow(i);
            String id = row.getCell(0).toString();
            String title = row.getCell(1).toString();
            String from = row.getCell(2).toString();
            String time = row.getCell(3).toString();
            String readCount = row.getCell(4).toString();
            String content = row.getCell(5).toString();
            Article article = new Article(id, title, from, time, readCount, content);
            articles.add(article);

            //一次插入1000条
            if(num == 1000){

                HbaseUtil.batchData(articles);
                num = 0;
                articles.clear();
            }
            num++;
        }
        return articles;
    }
}
