# Full Text Search Interface

This is an interface based on Apache Lucene for easy to code and easy to search.

## How to Use

```java
public class Entity {
    @Indexable(isKey = true)
    private String id;
    private String title;
    private String Content;
    
    // getters and setters
    // ...
}

Entity entity = new Entity();
entity.setId("<id>");
entity.setTitle("Hello World");
entity.setContent("This is the content.");

IndexService indexService = new IndexService("<path for index persistence>");
indexService.createIndex(entityInstance);
indexService.deleteIndex(Entity.class, "<id>");
SearchResult<Entity> matched = indexService.search("world", Entity.class, 1, 10);
```

Example Project: https://github.com/bndynet/web-framework-for-java

## Maven

More versions, please visit http://mvnrepository.com/artifact/net.bndy

```xml
<dependency>
    <groupId>net.bndy</groupId>
    <artifactId>ftsi</artifactId>
    <version>1.0</version>
</dependency>

```
