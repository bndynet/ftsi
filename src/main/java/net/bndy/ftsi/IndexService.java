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

    public IndexService(String dataDir) {
        this.analyzer = new StandardAnalyzer();
        this.dataDir = dataDir;
    }

    public <T> IndexStatus status(Class<T> clazz) {
        IndexReader reader = this.getReader(clazz);
        IndexStatus indexStatus = new IndexStatus(reader.numDocs(), reader.numDeletedDocs(), reader.maxDoc());
        try {
            reader.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return indexStatus;
    }

    public <T> int getTotals(Class<T> clazz) {
        return this.getReader(clazz).numDocs();
    }

    public int getTotals() {
        int totals = 0;
        if (!StringHelper.isNullOrWhiteSpace(this.dataDir) && IOHelper.isDirectoryExisted(this.dataDir)) {
            List<File> folders = IOHelper.getDirectories(this.dataDir);
            for (File file : folders) {
                totals += this.getReader(file.getName()).numDocs();
            }
        }
        return totals;
    }

    public void createIndex(Object... items) {
        Map<String, IndexWriter> writers = new HashMap<>();
        try {
            for (Object item : items) {
                IndexWriter writer = writers.get(item.getClass().getName());
                if (writer == null) {
                    writer = this.getWriter(item.getClass());
                    writers.put(item.getClass().getName(), writer);
                }

                Document doc = new Document();
                Field[] fields = item.getClass().getDeclaredFields();
                for (Field field : fields) {
                    field.setAccessible(true);
                    String fieldName = field.getName();
                    Type fieldType = field.getGenericType();
                    Object fieldValue = field.get(item);

                    if (fieldValue == null) {
                        continue;
                    }

                    Indexable annotationIndexable = AnnotationHelper.getFieldAnnotation(Indexable.class, item.getClass(), fieldName);
                    if (annotationIndexable != null && annotationIndexable.ignore() && !annotationIndexable.isKey()) {
                        continue;
                    }

                    if (annotationIndexable != null && annotationIndexable.isKey()) {
                        // for Key field
                        if (field.getType() != String.class) {
                            // key field must be String
                            throw new InvalidKeyTypeException(item.getClass());
                        }
                        // StringField can be used to identity the item to be deleted
                        doc.add(new StringField(fieldName, fieldValue.toString(), org.apache.lucene.document.Field.Store.YES));
                    } else if (fieldType.equals(String.class)) {
                        if (annotationIndexable != null && (annotationIndexable.stringIndexType() == IndexType.EXACT)) {
                            doc.add(new StringField(fieldName, fieldValue.toString(), org.apache.lucene.document.Field.Store.YES));
                        } else {
                            doc.add(new TextField(fieldName, fieldValue.toString(), org.apache.lucene.document.Field.Store.YES));
                        }
                    } else if (fieldType.equals(Long.class) || fieldType.equals(long.class)) {
                        // NumericDocValuesField is required for LongPoint, IntPoint, FloatPoint, DoublePoint... for sorting
                        // StoredField is required for LongPoint, IntPoint, FloatPoint, DoublePoint... for storing
                        Long val = Long.parseLong(fieldValue.toString());
                        doc.add(new NumericDocValuesField(fieldName, val));
                        doc.add(new StoredField(fieldName, val));
                        doc.add(new LongPoint(fieldName, val));
                    } else if (fieldType.equals(Integer.class) || fieldType.equals(int.class)) {
                        Integer val = Integer.parseInt(fieldValue.toString());
                        doc.add(new NumericDocValuesField(fieldName, val.longValue()));
                        doc.add(new StoredField(fieldName, val));
                        doc.add(new IntPoint(fieldName, val));
                    } else if (fieldType.equals(Float.class) || fieldType.equals(float.class)) {
                        Float val = Float.parseFloat(fieldValue.toString());
                        doc.add(new NumericDocValuesField(fieldName, val.longValue()));
                        doc.add(new StoredField(fieldName, val));
                        doc.add(new FloatPoint(fieldName, val));
                    } else if (fieldType.equals(Double.class) || fieldType.equals(double.class)) {
                        Double val = Double.parseDouble(fieldValue.toString());
                        doc.add(new NumericDocValuesField(fieldName, val.longValue()));
                        doc.add(new StoredField(fieldName, val));
                        doc.add(new DoublePoint(fieldName, val));
                    } else {
                        doc.add(new StringField(fieldName, fieldValue.toString(), org.apache.lucene.document.Field.Store.YES));
                    }
                }
                writer.addDocument(doc);
            }

            // close all writers
            for (IndexWriter writer : writers.values()) {
                writer.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void updateIndex(Object data) throws NoKeyDefinedException, IllegalAccessException {
        Field keyField = this.getKeyField(data.getClass());
        if (keyField == null) {
            throw new NoKeyDefinedException(data.getClass());
        }

        keyField.setAccessible(true);
        this.deleteIndex(data.getClass(), keyField.get(data));
        this.createIndex(data);
    }

    public <T> long deleteIndex(Class<T> clazz, Object keyValue) throws NoKeyDefinedException {
        if (keyValue == null || "".equals(keyValue.toString())) {
            return 0;
        }

        long result = 0;
        try {
            boolean hasKeyDefinition = false;
            IndexWriter writer = this.getWriter(clazz);
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                Indexable indexable = AnnotationHelper.getFieldAnnotation(Indexable.class, clazz, field.getName());
                if (indexable != null && indexable.isKey()) {
                    hasKeyDefinition = true;
                    Term term = new Term(field.getName(), keyValue.toString());
                    result = writer.deleteDocuments(term);
                    break;
                }
            }

            if (!hasKeyDefinition) {
                throw new NoKeyDefinedException(clazz);
            }
            writer.forceMergeDeletes();
            writer.close();
            return result;
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return result;
    }

    public <T> SearchResult<T> search(String fieldName, String fieldValue, Class<T> targetClass, int page, int pageSize) {
        if (page < 1) page = 1;
        if (pageSize < 1) pageSize = 10;
        List<T> items = new ArrayList<>();
        DirectoryReader reader = this.getReader(targetClass);
        IndexSearcher searcher = new IndexSearcher(reader);
        Query query = new TermQuery(new Term(fieldName, fieldValue));
        try {
            TopDocs topDocs = searcher.search(query, page * pageSize);
            ScoreDoc[] scoreDocs = topDocs.scoreDocs;
            for (int i = (page - 1) * pageSize; i < page * pageSize; i++) {
                if (i >= scoreDocs.length) {
                    break;
                }
                items.add(doc2Entity(searcher.doc(scoreDocs[i].doc), targetClass));
            }
            reader.close();
            return new SearchResult<>(page, pageSize, topDocs.totalHits > page * pageSize, items);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return null;
    }

    public <T> SearchResult<T> search(String keywords, Class<T> targetClass, int page, int pageSize) {
        return this.search(keywords, targetClass, null, page, pageSize);
    }

    public <T> SearchResult<T> search(String keywords, Class<T> targetClass, Map<String, Object> andCondition, int page, int pageSize) {
        if (page < 1) page = 1;
        if (pageSize < 1) pageSize = 10;
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

        try {
            Query query = MultiFieldQueryParser.parse(keywords,
                lstFields.toArray(new String[lstFields.size()]),
                lstOccurs.toArray(new BooleanClause.Occur[lstOccurs.size()]),
                analyzer);


            List<T> items = new ArrayList<>();
            DirectoryReader reader = this.getReader(targetClass);
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs topDocs = searcher.search(query, page * pageSize);
            ScoreDoc[] scoreDocs = topDocs.scoreDocs;

            for (int i = (page - 1) * pageSize; i < page * pageSize; i++) {
                if (i >= scoreDocs.length) {
                    break;
                }
                items.add(doc2Entity(searcher.doc(scoreDocs[i].doc), targetClass));
            }

            reader.close();
            return new SearchResult<>(page, pageSize, topDocs.totalHits > page * pageSize, items);
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ParseException ex) {
            ex.printStackTrace();
        }

        return null;
    }

    public <T> void deleteAll(Class<T> targetClass) {
        IndexWriter writer = this.getWriter(targetClass);
        try {
            writer.deleteAll();
            writer.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void deleteAll() {
        if (!StringHelper.isNullOrWhiteSpace(this.dataDir) && IOHelper.isDirectoryExisted(this.dataDir)) {
            List<File> folders = IOHelper.getDirectories(this.dataDir);
            for (File file : folders) {
                IndexWriter writer = this.getWriter(file.getName());
                try {
                    writer.deleteAll();
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private <T> Directory getCatalogDirectory(Class<T> targetClass) throws IOException {
        return this.getCatalogDirectory(targetClass.getName());
    }

    private <T> Directory getCatalogDirectory(String catalog) throws IOException {
        Path path = Paths.get(this.dataDir, catalog);
        return FSDirectory.open(path);
    }

    private <T> IndexWriter getWriter(Class<T> targetClass) {
        return this.getWriter(targetClass.getName());
    }

    private IndexWriter getWriter(String catalog) {
        try {
            IndexWriterConfig config = this.getIndexWriterConfig();
            config.setCommitOnClose(true);
            return new IndexWriter(this.getCatalogDirectory(catalog), config);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return null;
    }

    private <T> DirectoryReader getReader(Class<T> targetClass) {
        return this.getReader(targetClass.getName());
    }

    private DirectoryReader getReader(String catalog) {
        try {
            return DirectoryReader.open(this.getCatalogDirectory(catalog));
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    private List<String> getIndexableFields(Class<?> clazz) {
        return CollectionHelper.convert(CollectionHelper.filter(ReflectionHelper.getAllFields(clazz), (f) -> {
            Indexable indexable = AnnotationHelper.getFieldAnnotation(Indexable.class, clazz, f.getName());
            return indexable == null || !indexable.ignore();
        }), filed -> filed.getName());
    }

    private <T> T doc2Entity(Document doc, Class<T> targetClass) {
        try {
            return CollectionHelper.convertMap2(this.doc2Map(doc), targetClass);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return null;
    }

    private Map<String, Object> doc2Map(Document doc) {
        List<IndexableField> documentFields = doc.getFields();
        Map<String, Object> fieldMapping = new HashMap<>();
        for (IndexableField documentField : documentFields) {
            if (documentField.numericValue() != null) {
                fieldMapping.put(documentField.name(), documentField.numericValue());
            } else {
                fieldMapping.put(documentField.name(), documentField.stringValue());
            }
        }
        return fieldMapping;
    }

    private <T> Field getKeyField(Class<T> clazz) {
        return CollectionHelper.first(CollectionHelper.array2List(clazz.getDeclaredFields()), field -> {
            Indexable indexable = AnnotationHelper.getFieldAnnotation(Indexable.class, clazz, field.getName());
            return indexable != null && indexable.isKey();
        });
    }

    private IndexWriterConfig getIndexWriterConfig() {
        return new IndexWriterConfig(analyzer);
    }
}
