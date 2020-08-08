package org.apache.lucene.analysis.combo;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.combo.util.ReaderCloneFactory;

import java.io.IOException;
import java.io.Reader;
import java.util.Map;
import java.util.Set;

/**
 * An analyzer that combines multiple sub-analyzers into one.
 * <p>
 * It internally uses {@link ReaderCloneFactory} in order to feed the multiple
 * sub-analyzers from a single input.
 * If you analyzer big inputs or have a performance critical application,
 * please see the remarks of the latter's documentation.
 * <p>
 * The instances are thread safe with regards to the reused TokenStreams.
 * As duplicated sub-analyzer instance would break this safety,
 * there is a special case detection for it.
 * However, there can still be cases where an {@link Analyzer} is used from
 * multiple places at a time before the {@link ComboTokenStream} is fully consumed,
 * and this causes problem.
 * For a solution, see {@link #setTokenStreamCachingEnabled(boolean)}
 * and {@link #setTokenStreamCachingEnabled(boolean)}.
 */
public class ComboAnalyzer extends Analyzer {

    /**
     * Default value for the enabled state of {@link TokenStream} caching.
     */
    public static final boolean TOKENSTREAM_CACHING_ENABLED_DEFAULT = false;

    /**
     * Default value for the enabled state of token deduplication.
     */
    public static final boolean DEDUPLICATION_ENABLED_DEFAULT = false;

    private final Map<String, Set<Analyzer>> prefixVsAnalyzerSet;
    private final Set<Analyzer> defaultAnalyzerSet;
    private final boolean expectAnalyzerPrefix;
    private final String prefix;

    private boolean cacheTokenStreams = TOKENSTREAM_CACHING_ENABLED_DEFAULT;

    private boolean deduplication = DEDUPLICATION_ENABLED_DEFAULT;

    private boolean merge = false;
    private int positionGapBetweenAnalyzerTokens = 0;


    public ComboAnalyzer(String prefix, Map<String, Set<Analyzer>> prefixVsAnalyzerSet) {
        super();
        this.prefixVsAnalyzerSet = prefixVsAnalyzerSet;
        this.defaultAnalyzerSet = prefixVsAnalyzerSet.get(null);
        this.expectAnalyzerPrefix = prefixVsAnalyzerSet.size() > 1;

        if (expectAnalyzerPrefix) {
            this.prefix = (prefix == null) ? "" : prefix;
        } else {
            this.prefix = null;
        }
    }

    /**
     * Enable or disable the systematic caching of {@link Analyzer} {@link TokenStream}s.
     * <p>
     * {@link TokenStream}s gotten from the {@link Analyzer}s will be cached upfront.
     * This helps with one of the {@link Analyzer}s being reused before having completely
     * consumed our {@link ComboTokenStream}.
     * Note that this can happen too, if the same {@link Analyzer} instance is given twice.
     *
     * @param value {@code true} to enable the caching of {@link TokenStream}s,
     *              {@code false} to disable it.
     * @return This instance, for chainable construction.
     * @see #TOKENSTREAM_CACHING_ENABLED_DEFAULT
     */
    public ComboAnalyzer setTokenStreamCachingEnabled(boolean value) {
        cacheTokenStreams = value;
        return this;
    }

    /**
     * Enable the systematic caching of {@link Analyzer} {@link TokenStream}s.
     *
     * @return This instance, for chainable construction.
     * @see #setTokenStreamCachingEnabled(boolean)
     */
    public ComboAnalyzer enableTokenStreamCaching() {
        cacheTokenStreams = true;
        return this;
    }

    /**
     * Disable the systematic caching of {@link Analyzer} {@link TokenStream}s.
     *
     * @return This instance, for chainable construction.
     * @see #setTokenStreamCachingEnabled(boolean)
     */
    public ComboAnalyzer disableTokenStreamCaching() {
        cacheTokenStreams = false;
        return this;
    }

    /**
     * Enable or disable deduplication of repeated tokens at the same position.
     *
     * @param value {@code true} to enable the deduplication of tokens,
     *              {@code false} to disable it.
     * @return This instance, for chainable construction.
     * @see #DEDUPLICATION_ENABLED_DEFAULT
     */
    public ComboAnalyzer setDeduplicationEnabled(boolean value) {
        deduplication = value;
        return this;
    }

    /**
     * Enable deduplication of repeated tokens at the same position.
     *
     * @return This instance, for chainable construction.
     * @see #setDeduplicationEnabled(boolean)
     */
    public ComboAnalyzer enableDeduplication() {
        deduplication = true;
        return this;
    }

    /**
     * Disable deduplication of repeated tokens at the same position.
     *
     * @return This instance, for chainable construction.
     * @see #setDeduplicationEnabled(boolean)
     */
    public ComboAnalyzer disableDeduplication() {
        deduplication = false;
        return this;
    }

    public ComboAnalyzer positionGapBetweenAnalyzerTokens(int positionGapBetweenAnalyzerTokens) {
        this.positionGapBetweenAnalyzerTokens = positionGapBetweenAnalyzerTokens;
        return this;
    }

    public ComboAnalyzer merge(boolean merge) {
        this.merge = merge;
        return this;
    }

    private static Tokenizer DUMMY_TOKENIZER = new Tokenizer() {
        @Override
        public boolean incrementToken() throws IOException {
            return false;
        }
    };

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        return new CombiningTokenStreamComponents(fieldName);
    }

    @Override
    public void close() {
        super.close();
    }

    private class CombiningTokenStreamComponents extends TokenStreamComponents {

        private final String field;
        private Reader reader;

        public CombiningTokenStreamComponents(String field) {
            super(DUMMY_TOKENIZER);
            this.field = field;
        }

        @Override
        protected void setReader(Reader reader) {
            this.reader = reader;
        }

        @Override
        public TokenStream getTokenStream() {
            TokenStream ret = createTokenStreams();
            return deduplication ? new UniqueTokenFilter(ret) : ret;
        }

        private TokenStream createTokenStreams() {

            if (defaultAnalyzerSet.size() == 1 && !expectAnalyzerPrefix) {
                return createTokenStream(defaultAnalyzerSet.iterator().next(), field, reader);
            } else {
                ReaderCloneFactory.ReaderCloner<Reader> cloner = ReaderCloneFactory.getCloner(reader, prefix);
                Set<Analyzer> analyzers = prefixVsAnalyzerSet.getOrDefault(cloner.getValueAfterPrefix(), defaultAnalyzerSet);
                TokenStream[] streams = new TokenStream[analyzers.size()];
                int i = 0;

                for (Analyzer analyzer : analyzers) {
                    streams[i++] = createTokenStream(analyzer, field, cloner.giveAClone());
                }

                if (merge) {
                    return new ComboTokenStream(streams);
                } else {
                    return new ComboAppendedStream(positionGapBetweenAnalyzerTokens, streams);
                }
            }
        }

        private TokenStream createTokenStream(Analyzer analyzer, String field, Reader reader) {
            if (cacheTokenStreams) {
                return loadAndClose(analyzer.tokenStream(field, reader));
            } else {
                return analyzer.tokenStream(field, reader);
            }
        }

        private CachingTokenStream loadAndClose(TokenStream tokenStream) {
            CachingTokenStream cache = loadAsCaching(tokenStream);
            try {
                tokenStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return cache;
        }

        private CachingTokenStream loadAsCaching(TokenStream tokenStream) {
            try {
                CachingTokenStream cachingTokenStream = new CachingTokenStream(tokenStream);
                tokenStream.reset();
                cachingTokenStream.fillCache();
                return cachingTokenStream;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
