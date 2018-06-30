/**
 * Copyright (c) 2018 BNDY-NET. All Rights Reserved.
 * Created at 2018/6/29 1:28 PM
 * http://bndy.net
 */
package net.bndy.ftsi.test;

import net.bndy.ftsi.Indexable;

/**
 * @author Bendy Zhang 
 * @version 1.0
 */
public class IndexModel4Enum {

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Indexable(ignore = true, isKey = true)
    private String id;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    private String name;

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    private Type type;

    public enum Type {
        Article,
        SimplePage,
    }
}
