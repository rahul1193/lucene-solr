package org.apache.lucene.codecs;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class StoredFields {
    private int numDocsWritten = 0;
    private final ConcurrentMap<Integer, Document> documents = new ConcurrentHashMap<>();

    public Document addDocument() {
        Document document = new Document();
        documents.put(numDocsWritten, document);
        numDocsWritten++;
        return document;
    }

    public Document getDocument(int doc) {
        return documents.get(doc);
    }

    public static class Document {

        private final ConcurrentMap<Integer, Object> fieldValues = new ConcurrentHashMap<>();

        public void addField(int fieldId, Object value) {
            fieldValues.put(fieldId, value);
        }

        public ConcurrentMap<Integer, Object> getFieldValues() {
            return fieldValues;
        }
    }

    static StoredFields read(IndexInput indexInput) throws IOException {
        int numDocsWritten = indexInput.readVInt();
        StoredFields storedFields = new StoredFields();
        for (int i = 0; i < numDocsWritten; i++) {
            Document document = storedFields.addDocument();
            int docSize = indexInput.readVInt();
            for (int j = 0; j < docSize; j++) {
                int fieldId = indexInput.readVInt();
                int type = indexInput.readVInt();
                switch (type) {
                    case 1:
                        document.addField(fieldId, indexInput.readLong());
                        break;
                    case 2:
                        document.addField(fieldId, NumericUtils.sortableLongToDouble(indexInput.readLong()));
                        break;
                    case 3:
                        document.addField(fieldId, NumericUtils.sortableIntToFloat(indexInput.readInt()));
                        break;
                    case 4:
                        document.addField(fieldId, indexInput.readInt());
                        break;
                    case 5:
                        document.addField(fieldId, indexInput.readString());
                        break;
                    case 6:
                        int length = indexInput.readVInt();
                        byte[] value = new byte[length];
                        indexInput.readBytes(value, 0, length);
                        document.addField(fieldId, value);
                        break;
                }
            }
        }
        return storedFields;
    }


    public void write(IndexOutput output) throws IOException {
        output.writeVInt(numDocsWritten);
        for (int docId = 0; docId < numDocsWritten; docId++) {
            Document document = getDocument(docId);
            int size = document.getFieldValues().size();
            output.writeVInt(size);
            for (Map.Entry<Integer, Object> entry : document.getFieldValues().entrySet()) {
                output.writeVInt(entry.getKey());
                Object value = entry.getValue();
                if (value instanceof Number) {
                    if (value instanceof Long) {
                        output.writeVInt(1);
                        output.writeLong((Long) value);
                    } else if (value instanceof Double) {
                        output.writeVInt(2);
                        output.writeLong(NumericUtils.doubleToSortableLong((Double) value));
                    } else if (value instanceof Float) {
                        output.writeVInt(3);
                        output.writeInt(NumericUtils.floatToSortableInt((Float) value));
                    } else {
                        output.writeVInt(4);
                        output.writeInt((Integer) value);
                    }
                } else if (value instanceof String) {
                    output.writeVInt(5);
                    output.writeString(value.toString());
                } else if (value instanceof byte[]) {
                    output.writeVInt(6);
                    output.writeVInt(((byte[]) value).length);
                    output.writeBytes((byte[]) value, ((byte[]) value).length);
                }
            }
        }
    }
}
