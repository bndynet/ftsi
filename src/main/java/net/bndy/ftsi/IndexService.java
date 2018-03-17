package net.bndy.ftsi;

import net.bndy.lib.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class IndexService {

    private String dataDir;
    private Analyzer analyzer;

    public IndexService() {
        analyzer = new StandardAnalyzer();
    }

    public IndexService(String dataDir) {
        this();
        this.dataDir = dataDir;
    }

    public <T> int getTotals(Class<T> clazz) throws IOException {
        return this.getReader(clazz).numDocs();
    }

    public int getTotals() throws IOException {
        if (!StringHelper.isNullOrWhiteSpace(this.dataDir)) {
            int totals = 0;
            List<File> folders = IOHelper.getDirectories(this.dataDir);
            for (File file : folders) {
                totals += this.getReader(file.getName()).numDocs();
            }
            return totals;
        }

        return this.getReader().numDocs();
    }

    public void createIndex(Object data) {
        try {
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
                if (annotationIndexable != null && annotationIndexable.ignore()) {
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
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public <T> List<T> search(String fieldName, String fieldValue, Class<T> targetClass, int page, int pageSize) throws IOException, IllegalAccessException, ClassNotFoundException, InstantiationException {
        List<T> result = new ArrayList<>();
        DirectoryReader reader = this.getReader(targetClass);
        IndexSearcher searcher = new IndexSearcher(reader);
        Query query = new TermQuery(new Term(fieldName, fieldValue));
        TopDocs topDocs = searcher.search(query, page * pageSize);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for(int i = (page - 1); i < page * pageSize; i++ ) {
            if (i >= scoreDocs.length) {
                break;
            }
            result.add(doc2Entity(searcher.doc(scoreDocs[i].doc), targetClass));
        }
        reader.close();
        return result;
    }

    public <T> List<T> search(String keywords, Class<T> targetClass, int page, int pageSize) throws ParseException, IOException, IllegalAccessException, ClassNotFoundException, InstantiationException {
        return this.search(keywords, targetClass, null, page, pageSize);
    }

    public <T> List<T> search(String keywords, Class<T> targetClass, Map<String, Object> andCondition, int page, int pageSize) throws ParseException, IOException, IllegalAccessException, ClassNotFoundException, InstantiationException {
        List<String> lstFields = this.getIndexableFields(targetClass);
        List<BooleanClause.Occur> lstOccurs = new ArrayList<>();
        for (String field : lstFields) {
            lstOccurs.add(BooleanClause.Occur.SHOULD);
        }

        if (andCondition != null) {
            for (String key : andCondition.keySet()) {
                lstOccurs.add(BooleanClause.Occur.MUST);
                lstFields.add(key);
            }
        }

        Query query = MultiFieldQueryParser.parse(keywords,
            lstFields.toArray(new String[lstFields.size()]),
            lstOccurs.toArray(new BooleanClause.Occur[lstOccurs.size()]),
            analyzer);


        List<T> result = new ArrayList<>();
        DirectoryReader reader = this.getReader(targetClass);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(query, page * pageSize);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;

        for(int i = (page - 1); i < page * pageSize; i++ ) {
            if (i >= scoreDocs.length) {
                break;
            }
            result.add(doc2Entity(searcher.doc(scoreDocs[i].doc), targetClass));
        }

        reader.close();
        return result;
    }

    public <T> void deleteAll(Class<T> targetClass) throws IOException {
        IndexWriter writer = this.getWriter(targetClass);
        writer.deleteAll();
        writer.close();
    }

    public void deleteAll() {
        // TODO: clear all indexes
    }

    private Directory ramDirectory;

    private Directory getDirectory() throws IOException {
        if (StringHelper.isNullOrWhiteSpace(this.dataDir)) {
            if (ramDirectory == null)
                ramDirectory = new RAMDirectory();
            return ramDirectory;
        }

        return FSDirectory.open(Paths.get(this.dataDir));
    }

    private <T> Directory getCatalogDirectory(Class<T> targetClass) throws IOException {
        if (StringHelper.isNullOrWhiteSpace(this.dataDir)) {
            if (ramDirectory == null)
                ramDirectory = new RAMDirectory();
            return ramDirectory;
        }

        Path path = Paths.get(this.dataDir, targetClass.getName());
        return FSDirectory.open(path);
    }

    private Directory getCatalogDirectory(String catalog) throws IOException {
        if (StringHelper.isNullOrWhiteSpace(this.dataDir)) {
            if (ramDirectory == null)
                ramDirectory = new RAMDirectory();
            return ramDirectory;
        }

        return FSDirectory.open(Paths.get(this.dataDir, catalog));
    }

    private <T> IndexWriter getWriter(Class<T> targetClass) throws IOException {
        IndexWriterConfig writerConfig = new IndexWriterConfig(analyzer);
        return new IndexWriter(this.getCatalogDirectory(targetClass), writerConfig);
    }

    public DirectoryReader getReader() throws IOException {
        return DirectoryReader.open(this.getDirectory());
    }

    private <T> DirectoryReader getReader(Class<T> targetClass) throws IOException {
        return DirectoryReader.open(this.getCatalogDirectory(targetClass));
    }

    private DirectoryReader getReader(String catalog) throws IOException {
        return DirectoryReader.open(this.getCatalogDirectory(catalog));
    }

    private List<String> getIndexableFields(Class<?> clazz) {
        return CollectionHelper.convert(CollectionHelper.filter(ReflectionHelper.getAllFields(clazz), (f) -> {
            Indexable indexable = AnnotationHelper.getFieldAnnotation(Indexable.class, clazz, f.getName());
            return indexable == null || !indexable.ignore();
        }), filed -> filed.getName());
    }

    private <T> T doc2Entity(Document doc, Class<T> targetClass) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        return CollectionHelper.convertMap2(this.doc2Map(doc), targetClass);
    }

    private Map<String, Object> doc2Map(Document doc) {
        List<IndexableField> documentFields = doc.getFields();
        Map<String, Object> fieldMapping = new HashMap<>();
        for (IndexableField documentField : documentFields) {
            fieldMapping.put(documentField.name(), documentField.stringValue());
        }
        return fieldMapping;
    }
}
