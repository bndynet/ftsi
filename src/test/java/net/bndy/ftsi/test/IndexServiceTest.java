package net.bndy.ftsi.test;

import net.bndy.ftsi.IndexService;
import net.bndy.ftsi.IndexStatus;
import net.bndy.ftsi.NoKeyDefinedException;
import net.bndy.ftsi.SearchResult;
import net.bndy.lib.IOHelper;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.List;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IndexServiceTest {

    private static List<IndexModel4Test> models = new ArrayList<>();
    private static List<IndexModel4Test2> models2 = new ArrayList<>();
    private static IndexService indexService;
    private static String dataDir = "./index_dir";


    @BeforeClass public static void init() {
        IndexModel4Test m1 = new IndexModel4Test();
        m1.setId("1");
        m1.setTitle("Hello World");
        m1.setContent("This is a testing data.");
        models.add(m1);

        IndexModel4Test m2 = new IndexModel4Test();
        m2.setId("2");
        m2.setTitle("Hello");
        m2.setContent("Index data 2");
        models.add(m2);

        IndexModel4Test m3 = new IndexModel4Test();
        m3.setId("3");
        m3.setTitle("hi");
        m3.setContent("Index data 3");
        models.add(m3);

        IndexModel4Test2 mm3 = new IndexModel4Test2();
        mm3.setId(4);
        mm3.setTitle("hi");
        mm3.setContent("Index data 3");
        models2.add(mm3);

        indexService = new IndexService(dataDir);
        for(IndexModel4Test m: models) {
            indexService.createIndex(m);
        }
        for(IndexModel4Test2 m: models2) {
            indexService.createIndex(m);
        }
    }

    @AfterClass public static void destroy() {
        indexService.deleteAll();
        Assert.assertEquals(indexService.getTotals(), 0);

        IOHelper.forceDelete(dataDir);
        Assert.assertEquals(IOHelper.isDirectoryExisted(dataDir), false);
    }


    @Test
    public void t1_testSearch() {
        SearchResult<IndexModel4Test> matched = indexService.search("title", "hi", IndexModel4Test.class, 1, 10);
        Assert.assertEquals(matched.getContent().size(), 1);
        matched = indexService.search("World", IndexModel4Test.class, 1, 10);
        Assert.assertEquals(matched.getContent().size(), 1);
        Assert.assertEquals(indexService.getTotals(IndexModel4Test.class), 3);

        SearchResult<IndexModel4Test2> matched2 = indexService.search("title", "hi", IndexModel4Test2.class, 1, 10);
        Assert.assertEquals(matched2.getContent().size(), 1);
        Assert.assertEquals(indexService.getTotals(IndexModel4Test2.class), 1);

        Assert.assertEquals(indexService.getTotals(), 4);
    }

    @Test
    public void t2_testDelete() throws NoKeyDefinedException {
        long deleted = indexService.deleteIndex(IndexModel4Test.class, "1");
        Assert.assertEquals(deleted, 1);
        Assert.assertEquals(indexService.status(IndexModel4Test.class).getTotal(), 2);
    }

    @Test
    public void t3_testUpdate() throws NoKeyDefinedException, IllegalAccessException {
        SearchResult<IndexModel4Test> matchedBeforeUpdate = indexService.search("id", "1", IndexModel4Test.class, 1, 10);
        Assert.assertEquals(matchedBeforeUpdate.getContent().size(), 0);

        IndexModel4Test newModel = new IndexModel4Test();
        newModel.setId("1");
        newModel.setTitle("Updated");
        newModel.setContent("This is an updated item。");
        indexService.updateIndex(newModel);

        IndexStatus status = indexService.status(IndexModel4Test.class);
        Assert.assertEquals(status.getNum(), 3);

        SearchResult<IndexModel4Test> matchedAfterUpdate = indexService.search("id", "1", IndexModel4Test.class, 1, 10);
        Assert.assertEquals(matchedAfterUpdate.getContent().size(), 1);
    }
}

