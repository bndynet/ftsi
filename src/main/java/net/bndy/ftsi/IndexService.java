package net.bndy.ftsi;

import net.bndy.lib.AnnotationHelper;
import net.bndy.lib.StringHelper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexService {

    public IndexWriter writer;
    private String dataPath = "";
    private Analyzer analyzer;
    private List<String> fields;

    public IndexService() throws IOException {
        analyzer = new StandardAnalyzer();
    }

    public void createIndex(Object data) throws IOException, IllegalAccessException, NoSuchFieldException {
        IndexWriter writer = this.getWriter(data.getClass().getName());
        Document doc = new Document();
        Field[] fields = data.getClass().getDeclaredFields();
        for (Field field : fields) {
            String fieldName = field.getName();
            Type fieldType = field.getGenericType();
            Object fieldValue = field.get(data);

            if (fieldValue == null) {
                continue;
            }

            Indexable annotationIndexable = null; // AnnotationHelper.getFieldAnnotation(Indexable.class, data.getClass(), fieldName);
            if (annotationIndexable !=null && annotationIndexable.disabled()) {
                continue;
            }

            field.setAccessible(true);
            if (fieldType.equals(String.class)) {
                if (annotationIndexable != null && annotationIndexable.indexType() == IndexType.EXACT) {
                    doc.add(new StringField(fieldName, field.get(data).toString(), org.apache.lucene.document.Field.Store.YES));
                } else {
                    doc.add(new TextField(fieldName, field.get(data).toString(), org.apache.lucene.document.Field.Store.YES));
                }
            } else if (fieldType.equals(Long.class) || fieldType.equals(long.class)) {
                doc.add(new LongPoint(fieldName, Long.parseLong(field.get(data).toString())));
            } else if (fieldType.equals(Integer.class) || fieldType.equals(int.class)) {
                doc.add(new IntPoint(fieldName, field.getInt(data)));
            } else if (fieldType.equals(Float.class) || fieldType.equals(float.class)) {
                doc.add(new FloatPoint(fieldName, field.getFloat(data)));
            } else if (fieldType.equals(Double.class) || fieldType.equals(double.class)) {
                doc.add(new DoublePoint(fieldName, field.getDouble(data)));
            } else {
                doc.add(new StringField(fieldName, field.get(data).toString(), org.apache.lucene.document.Field.Store.YES));
            }
        }
        writer.addDocument(doc);
        writer.commit();
        writer.close();
    }

    public void query(String keywords, String category) {
        String[] words = StringHelper.splitWithoutWhitespace(keywords, " ");

    }

    public List<Map<String, Object>> search(String fieldName, String fieldValue, String category) throws IOException {
        List<Map<String, Object>> result = new ArrayList<>();
        DirectoryReader reader = this.getReader(category);
        IndexSearcher searcher = new IndexSearcher(reader);
        Query query = new TermQuery(new Term(fieldName, fieldValue));
        TopDocs topDocs = searcher.search(query, 10);
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            Map<String, Object> item = new HashMap<>();
            Document document = searcher.doc(scoreDoc.doc);
            List<IndexableField> documentFields = document.getFields();
            for (IndexableField documentField : documentFields) {
                item.put(documentField.name(), documentField.stringValue());
            }
            result.add(item);
        }
        reader.close();
        return result;
    }

    public void deleteAll(String category) throws IOException {
        IndexWriter writer = this.getWriter(category);
        writer.deleteAll();
        writer.close();
    }

    public void deleteAll() throws IOException {
        this.deleteAll(null);
    }

    private Directory d;
    private Directory getCategoryDirectory(String category) throws IOException {
        //soluation: RAMDirectory
        if (d == null) {
            d = new RAMDirectory();
        }
        return d;
//        Path path = Paths.get(dataPath, category);
//        return FSDirectory.open(path);
    }

    private IndexWriter getWriter(String category) throws IOException {
        IndexWriterConfig writerConfig = new IndexWriterConfig(analyzer);
        return new IndexWriter(this.getCategoryDirectory(category), writerConfig);
    }

    private DirectoryReader getReader(String category) throws IOException {
        return DirectoryReader.open(this.getCategoryDirectory(category));
    }

    private <T> Field[] getFieldsOfT(Class<T> clazz) {
        return clazz.getDeclaredFields();
    }


}
