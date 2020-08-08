package org.apache.lucene.analysis.combo.util;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

/**
 * Default, memory costly but generic implementation of a {@link Reader} duplicator.
 * <p>
 * This implementation makes no assumption on the initial Reader.
 * Therefore, only the read() functions are available to figure out
 * what was the original content provided to the initial Reader.
 * <p>
 * After having read and filled a buffer with the whole content,
 * a String-based Reader implementation will be used and returned.
 * <p>
 * This implementation is memory costly because the initial content is
 * forcefully duplicated once. Moreover, buffer size growth may cost
 * some more memory too.
 *
 * @author ofavre
 */
public class ReaderClonerDefaultImpl implements ReaderCloneFactory.ReaderCloner<Reader> {

    public static final int DEFAULT_INITIAL_CAPACITY = 64 * 1024;
    public static final int DEFAULT_READ_BUFFER_SIZE = 16 * 1024;

    protected int initialCapacity;
    protected int readBufferSize;

    private String originalContent;
    private String prefix;

    public ReaderClonerDefaultImpl() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_READ_BUFFER_SIZE);
    }

    public ReaderClonerDefaultImpl(int initialCapacity) {
        this(initialCapacity, DEFAULT_READ_BUFFER_SIZE);
    }

    /**
     * Extracts the original content from a generic Reader instance
     * by repeatedly calling {@link Reader#read(char[])} on it,
     * feeding a {@link StringBuilder}.
     *
     * @param initialCapacity Initial StringBuilder capacity
     * @param readBufferSize Size of the char[] read buffer at each read() call
     */
    public ReaderClonerDefaultImpl(int initialCapacity, int readBufferSize) {
        this.initialCapacity = initialCapacity;
        this.readBufferSize = readBufferSize;
    }

    public void init(Reader originalReader, String expectedPrefix) throws IOException {
        this.originalContent = null;
        StringBuilder sb;
        if (initialCapacity < 0) {
            sb = new StringBuilder();
        } else {
            sb = new StringBuilder(initialCapacity);
        }
        char[] buffer = new char[readBufferSize];
        int read;
        while ((read = originalReader.read(buffer)) != -1) {
            sb.append(buffer, 0, read);
        }
        this.originalContent = sb.toString();
        originalReader.close();

        if (expectedPrefix != null) {
            Tuple<String, String> tuple = ReaderCloneFactory.stripPrefixIfPresent(originalContent, expectedPrefix);
            originalContent = tuple.v2();
            prefix = tuple.v1();
        }
    }

    /**
     * Returns a new {@link StringReader} instance,
     * directly based on the extracted original content.
     *
     * @return A {@link StringReader}
     */
    @Override
    public Reader giveAClone() {
        return new StringReader(originalContent);
    }

    @Override
    public String getValueAfterPrefix() {
        return prefix;
    }
}
