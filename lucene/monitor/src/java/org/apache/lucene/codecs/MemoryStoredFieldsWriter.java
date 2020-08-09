package org.apache.lucene.codecs;

import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;

class MemoryStoredFieldsWriter extends StoredFieldsWriter {

    private final String codecName;
    private final StoredFields storedFields;
    private final SegmentInfo segmentInfo;
    private StoredFields.Document currentDoc;
    private IndexOutput output = null;


    MemoryStoredFieldsWriter(String codecName, Directory directory, SegmentInfo segmentInfo, IOContext ioContext) throws IOException {
        String fileName = IndexFileNames.segmentFileName(segmentInfo.name, "", "mapFld");
        this.codecName = codecName;
        this.segmentInfo = segmentInfo;
        this.storedFields = new StoredFields();
        boolean success = false;
        try {
            output = directory.createOutput(fileName, ioContext);
            success = true;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(output);
            }
        }
    }

    @Override
    public void startDocument() {
        currentDoc = storedFields.addDocument();
    }

    @Override
    public void writeField(FieldInfo info, IndexableField field) throws IOException {
        if (field.numericValue() != null) {
            currentDoc.addField(info.number, field.numericValue());
        } else if (field.binaryValue() != null) {
            currentDoc.addField(info.number, BytesRef.deepCopyOf(field.binaryValue()).bytes);
        } else if (field.stringValue() != null) {
            currentDoc.addField(info.number, field.stringValue());
        }
    }

    @Override
    public void finish(FieldInfos fieldInfos, int doc) throws IOException {
        currentDoc = null;
    }

    @Override
    public void close() throws IOException {
        CodecUtil.writeIndexHeader(output, codecName, 1, segmentInfo.getId(), "");
        storedFields.write(output);
        CodecUtil.writeFooter(output);
        IOUtils.closeWhileHandlingException(output);
    }
}
