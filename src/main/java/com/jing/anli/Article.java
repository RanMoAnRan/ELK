package com.jing.anli;

import java.io.Serializable;

/**
 * @Date 2019/8/2
 */
public class Article implements Serializable {
    private String id;
    private String title ;
    private String from;
    private String time;
    private String readCount;
    private String content;



    public Article() {
    }


    public Article(String id, String title, String from, String time, String readCount, String content) {
        this.id = id;
        this.title = title;
        this.from = from;
        this.time = time;
        this.readCount = readCount;
        this.content = content;
    }

    @Override
    public String toString() {
        return "Article{" +
                "id='" + id + '\'' +
                ", title='" + title + '\'' +
                ", from='" + from + '\'' +
                ", time='" + time + '\'' +
                ", readCount='" + readCount + '\'' +
                ", content='" + content + '\'' +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getReadCount() {
        return readCount;
    }

    public void setReadCount(String readCount) {
        this.readCount = readCount;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
