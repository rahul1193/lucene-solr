package org.apache.lucene.analysis.combo.util;

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Field;

/**
 * A ReaderCloner specialized for CharArrayReader.
 * <p>
 * The only efficient mean of retrieving the original content
 * from a CharArrayReader is to use introspection and access the
 * {@code private String str} field.
 * <p>
 * Apart from being efficient, this code is also very sensitive
 * to the used JVM implementation.
 * If the introspection does not work, an {@link IllegalArgumentException}
 * is thrown.
 */
public class CharArrayReaderCloner implements ReaderCloneFactory.ReaderCloner<CharArrayReader> {

    private static Field internalField;

    private CharArrayReader original;
    private char[] originalContent;
    private String prefix;

    static {
        try {
            internalField = CharArrayReader.class.getDeclaredField("buf");
            internalField.setAccessible(true);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Could not give accessibility to private \"buf\" field of the given CharArrayReader", ex);
        }
    }

    public void init(CharArrayReader originalReader, String expectedPrefix) throws IOException {
        this.original = originalReader;
        this.originalContent = null;
        try {
            this.originalContent = (char[]) internalField.get(original);
            if (expectedPrefix != null) {
                Tuple<String, String> tuple = ReaderCloneFactory.stripPrefixIfPresent(new String(originalContent), expectedPrefix);
                originalContent = tuple.v2().toCharArray();
                prefix = tuple.v1();
                this.original = new CharArrayReader(originalContent);
            }

        } catch (Exception ex) {
            throw new IllegalArgumentException("Could not access private \"buf\" field of the given CharArrayReader (actual class: " + original.getClass().getCanonicalName() + ")", ex);
        }
    }

    /**
     * First call will return the original Reader provided.
     */
    public Reader giveAClone() {
        if (original != null) {
            Reader rtn = original;
            original = null; // no longer hold a reference
            return rtn;
        }
        return new CharArrayReader(originalContent);
    }

    @Override
    public String getValueAfterPrefix() {
        return prefix;
    }
}
