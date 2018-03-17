package net.bndy.ftsi.test;

import net.bndy.ftsi.IndexService;
import net.bndy.lib.IOHelper;
import org.apache.lucene.queryparser.classic.ParseException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IndexServiceTest {

    private static List<IndexModel4Test> models = new ArrayList<>();
    private static List<IndexModel4Test2> models2 = new ArrayList<>();


    @BeforeClass public static void init() {
        IndexModel4Test m1 = new IndexModel4Test();
        m1.setId(1l);
        m1.setTitle("Hello World");
        m1.setContent("This is a testing data.");
        IndexModel4Test m2 = new IndexModel4Test();
        m2.setId(2l);
        m2.setTitle("Hello");
        m2.setContent("Index data 2");
        IndexModel4Test m3 = new IndexModel4Test();
        m3.setId(3l);
        m3.setTitle("hi");
        m3.setContent("Index data 3");
        models.add(m1);
        models.add(m2);
        models.add(m3);

        IndexModel4Test2 mm3 = new IndexModel4Test2();
        mm3.setId(3l);
        mm3.setTitle("hi");
        mm3.setContent("Index data 3");
        models2.add(mm3);
    }

    @Test public void indexWithRAM() throws IOException, IllegalAccessException, ParseException, ClassNotFoundException, InstantiationException {
        IndexService service = new IndexService();

        for(IndexModel4Test m: this.models) {
            service.createIndex(m);
        }
        List<IndexModel4Test> matched = service.search("title", "hi", IndexModel4Test.class, 1, 10);
        Assert.assertEquals(matched.size(), 1);
        matched = service.search("Hello World", IndexModel4Test.class, 1, 10);
        Assert.assertEquals(matched.size(), 2);
        Assert.assertEquals(service.getTotals(IndexModel4Test.class), 3);


        for(IndexModel4Test2 m: this.models2) {
            service.createIndex(m);
        }
        List<IndexModel4Test2> matched2 = service.search("title", "hi", IndexModel4Test2.class, 1, 10);
        Assert.assertEquals(matched2.size(), 2);
        Assert.assertEquals(service.getTotals(IndexModel4Test2.class), 4);

        Assert.assertEquals(service.getTotals(), 4);
    }

    @Test public void indexWithFolder() throws IOException, IllegalAccessException, ParseException, ClassNotFoundException, InstantiationException {
        String dataDir = "./index_dir";
        IndexService service = new IndexService(dataDir);
        for(IndexModel4Test m: this.models) {
            service.createIndex(m);
        }

        List<IndexModel4Test> matched = service.search("title", "hi", IndexModel4Test.class, 1, 10);
        Assert.assertEquals(matched.size(), 1);
        matched = service.search("Hello World", IndexModel4Test.class, 1, 10);
        Assert.assertEquals(matched.size(), 2);
        Assert.assertEquals(service.getTotals(IndexModel4Test.class), 3);


        for(IndexModel4Test2 m: this.models2) {
            service.createIndex(m);
        }
        List<IndexModel4Test2> matched2 = service.search("title", "hi", IndexModel4Test2.class, 1, 10);
        Assert.assertEquals(matched2.size(), 1);
        Assert.assertEquals(service.getTotals(IndexModel4Test2.class), 1);

        Assert.assertEquals(service.getTotals(), 4);

        IOHelper.forceDelete(dataDir);
    }
}

