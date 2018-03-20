package net.bndy.ftsi.test;

import net.bndy.ftsi.Indexable;

public class IndexModel4Test {
    @Indexable(ignore = true, isKey = true)
    private String id;
    private String title;

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

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    private String content;
}
