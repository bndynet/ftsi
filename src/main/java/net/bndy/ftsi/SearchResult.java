package net.bndy.ftsi;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SearchResult<T> implements Serializable {

    private int page;
    private int pageSize;
    private boolean hasMore;
    private List<T> content;

    public int getPage() {
        return page;
    }

    public int getPageSize() {
        return pageSize;
    }

    public boolean hasMore() {
        return hasMore;
    }

    public List<T> getContent() {
        return content;
    }


    public SearchResult(int page, int pageSize, boolean hasMore) {
        this(page, pageSize, hasMore, new ArrayList<>());
    }

    public SearchResult(int page, int pageSize, boolean hasMore, List<T> content) {
        this.page = page;
        this.pageSize = pageSize;
        this.hasMore = hasMore;
        this.content = content;
    }

    public void appendContent(T entity) {
        this.content.add(entity);
    }
}
