package org.apache.lucene.codecs;

import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;

public class MemoryStoredFieldsFormat extends StoredFieldsFormat {

    private final String codecName;

    public MemoryStoredFieldsFormat(String codecName) {
        this.codecName = codecName;
    }

    @Override
    public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo segmentInfo, FieldInfos fieldInfos, IOContext ioContext)
            throws IOException {
        String fileName = IndexFileNames.segmentFileName(segmentInfo.name, "", "mapFld");
        StoredFields storedFields = null;
        IndexInput input = null;
        try {
            input = directory.openInput(fileName, ioContext);
            CodecUtil.readIndexHeader(input);
            storedFields = StoredFields.read(input);
            CodecUtil.readFooter(input);
        } finally {
            IOUtils.closeWhileHandlingException(input);
        }
        return new MemoryStoredFieldsReader(fieldInfos, storedFields);
    }

    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo segmentInfo, IOContext ioContext) throws IOException {
        return new MemoryStoredFieldsWriter(codecName, directory, segmentInfo, ioContext);
    }
}
