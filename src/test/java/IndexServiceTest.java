import net.bndy.ftsi.IndexService;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class IndexServiceTest {

    @Test public void search() throws IOException, NoSuchFieldException, IllegalAccessException {

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


        IndexService service = new IndexService();
        service.createIndex(m1);
        service.createIndex(m2);
        service.createIndex(m3);

        List<Map<String, Object>> matched = service.search("title", "hi", null);
        Assert.assertEquals(matched.size(), 1);
    }
}

