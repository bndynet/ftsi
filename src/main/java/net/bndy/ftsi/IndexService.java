package net.bndy.ftsi;

import net.bndy.lib.AnnotationHelper;
import net.bndy.lib.CollectionHelper;
import net.bndy.lib.ReflectionHelper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.*;

public class IndexService {

    private String dataPath = "";
    private Analyzer analyzer;

    public IndexService() {
        analyzer = new StandardAnalyzer();
    }

    public void createIndex(Object data) throws IOException, IllegalAccessException {
        IndexWriter writer = this.getWriter(data.getClass());
        Document doc = new Document();
        Field[] fields = data.getClass().getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            String fieldName = field.getName();
            Type fieldType = field.getGenericType();
            Object fieldValue = field.get(data);

            if (fieldValue == null) {
                continue;
            }

            Indexable annotationIndexable = AnnotationHelper.getFieldAnnotation(Indexable.class, data.getClass(), fieldName);
            if (annotationIndexable !=null && annotationIndexable.ignore()) {
                continue;
            }

            if (fieldType.equals(String.class)) {
                if (annotationIndexable != null && annotationIndexable.stringIndexType() == IndexType.EXACT) {
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
        writer.close();
    }

    public <T> List<T> search(String fieldName, String fieldValue, Class<T> targetClass) throws IOException, IllegalAccessException, ClassNotFoundException, InstantiationException {
        List<T> result = new ArrayList<>();
        DirectoryReader reader = this.getReader(targetClass);
        IndexSearcher searcher = new IndexSearcher(reader);
        Query query = new TermQuery(new Term(fieldName, fieldValue));
        TopDocs topDocs = searcher.search(query, 10);
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            result.add(doc2Entity(searcher.doc(scoreDoc.doc), targetClass));
        }
        reader.close();
        return result;
    }

    public <T> List<T> search(String keywords, Class<T> targetClass) throws ParseException, IOException, IllegalAccessException, ClassNotFoundException, InstantiationException {
        List<String> lstFields = this.getIndexableFields(targetClass);
        List<BooleanClause.Occur> lstOccurs = new ArrayList<>();
        for(String field: lstFields) {
            lstOccurs.add(BooleanClause.Occur.SHOULD);
        }

        Query query = MultiFieldQueryParser.parse(keywords,
            lstFields.toArray(new String[lstFields.size()]),
            lstOccurs.toArray(new BooleanClause.Occur[lstOccurs.size()]),
            analyzer);

        List<T> result = new ArrayList<>();
        DirectoryReader reader = this.getReader(targetClass);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(query, 10);
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            result.add(doc2Entity(searcher.doc(scoreDoc.doc), targetClass));
        }
        reader.close();
        return result;
    }


    public <T> void deleteAll(Class<T> targetClass) throws IOException {
        IndexWriter writer = this.getWriter(targetClass);
        writer.deleteAll();
        writer.close();
    }

    public void deleteAll() throws IOException {
        this.deleteAll(null);
    }

    private Directory d;
    private <T> Directory getCategoryDirectory(Class<T> targetClass) throws IOException {
        //soluation: RAMDirectory
        if (d == null) {
            d = new RAMDirectory();
        }
        return d;
//        Path path = Paths.get(dataPath, category);
//        return FSDirectory.open(path);
    }

    private <T> IndexWriter getWriter(Class<T> targetClass) throws IOException {
        IndexWriterConfig writerConfig = new IndexWriterConfig(analyzer);
        return new IndexWriter(this.getCategoryDirectory(targetClass), writerConfig);
    }

    private <T> DirectoryReader getReader(Class<T> targetClass) throws IOException {
        return DirectoryReader.open(this.getCategoryDirectory(targetClass));
    }

    private List<String> getIndexableFields(Class<?> clazz) {
        return CollectionHelper.convert(CollectionHelper.filter(ReflectionHelper.getAllFields(clazz), (f) -> {
            Indexable indexable = AnnotationHelper.getFieldAnnotation(Indexable.class, clazz, f.getName());
            return indexable == null || !indexable.ignore();
        }), filed -> filed.getName());
    }

    private <T> T doc2Entity(Document doc, Class<T> targetClass) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        List<IndexableField> documentFields = doc.getFields();
        Map<String, Object> fieldMapping = new HashMap<>();
        for (IndexableField documentField : documentFields) {
            fieldMapping.put(documentField.name(), documentField.stringValue());
        }
        return CollectionHelper.convertMap2(fieldMapping, targetClass);
    }
}
