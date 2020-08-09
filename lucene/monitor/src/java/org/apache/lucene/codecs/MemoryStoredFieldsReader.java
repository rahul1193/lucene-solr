package org.apache.lucene.codecs;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.StoredFieldVisitor;

import java.io.IOException;
import java.util.Map;

class MemoryStoredFieldsReader extends StoredFieldsReader {

    private final FieldInfos fieldInfos;
    private final StoredFields storedFields;

    MemoryStoredFieldsReader(FieldInfos fieldInfos, StoredFields storedFields) {
        this.fieldInfos = fieldInfos;
        this.storedFields = storedFields;
    }

    @Override
    public void visitDocument(int doc, StoredFieldVisitor storedFieldVisitor) throws IOException {
        if (storedFields == null) {
            return;
        }
        StoredFields.Document document = storedFields.getDocument(doc);
        if (document == null) {
            return;
        }
        for (Map.Entry<Integer, Object> entry : document.getFieldValues().entrySet()) {
            Integer fieldId = entry.getKey();
            FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldId);
            switch (storedFieldVisitor.needsField(fieldInfo)) {
                case YES:
                    Object value = entry.getValue();
                    consumeValue(fieldInfo, storedFieldVisitor, value);
                    continue;
                case STOP:
                    break;
            }
        }
    }

    @Override
    public StoredFieldsReader clone() {
        return this;
    }

    @Override
    public void checkIntegrity() {
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    @Override
    public void close() throws IOException {
    }

    private void consumeValue(FieldInfo fieldInfo, StoredFieldVisitor storedFieldVisitor, Object value) throws IOException {
        if (value instanceof String) {
            storedFieldVisitor.stringField(fieldInfo, ((String) value));
        } else if (value instanceof byte[]) {
            storedFieldVisitor.binaryField(fieldInfo, (byte[]) value);
        } else if (value instanceof Long) {
            storedFieldVisitor.longField(fieldInfo, (Long) value);
        } else if (value instanceof Double) {
            storedFieldVisitor.doubleField(fieldInfo, (Double) value);
        } else if (value instanceof Float) {
            storedFieldVisitor.floatField(fieldInfo, (Float) value);
        } else if (value instanceof Integer) {
            storedFieldVisitor.intField(fieldInfo, (Integer) value);
        }
    }
}
