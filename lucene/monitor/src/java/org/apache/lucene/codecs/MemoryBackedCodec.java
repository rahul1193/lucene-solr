package org.apache.lucene.codecs;

import org.apache.lucene.codecs.lucene86.Lucene86Codec;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;

public class MemoryBackedCodec extends FilterCodec {

    private static final String CODEC_NAME = "LuceneMainMemoryCodec";

    private final StoredFieldsFormat storedFieldsFormat;
    private final PostingsFormat directPostingsFormat;
    private final PostingsFormat postingsFormat;

    public MemoryBackedCodec() {
        super("LuceneMainMemoryCodec", new Lucene86Codec());
        this.directPostingsFormat = PostingsFormat.forName("Direct");
        this.postingsFormat = new PerFieldPostingsFormat() {
            @Override
            public PostingsFormat getPostingsFormatForField(String field) {
                return MemoryBackedCodec.this.getPostingsFormatForField(field);
            }
        };
        this.storedFieldsFormat = new MemoryStoredFieldsFormat(CODEC_NAME);
    }

    @Override
    public PostingsFormat postingsFormat() {
        return postingsFormat;
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    public PostingsFormat getPostingsFormatForField(String field) {
        return directPostingsFormat;
    }
}
