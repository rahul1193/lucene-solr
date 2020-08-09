package org.apache.lucene.monitor;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;

public class QueryManager implements Closeable {

    private final QueryIndex queryIndex;

    private final List<MonitorUpdateListener> listeners = new ArrayList<>();

    protected final QueryDecomposer decomposer;
    private final Presearcher presearcher;

    private final ScheduledExecutorService purgeExecutor;
    private final int commitBatchSize;

    private long lastPurged = -1;

    public QueryManager(Presearcher presearcher, MonitorConfiguration configuration) throws IOException {
        this.presearcher = presearcher;
        this.decomposer = configuration.getQueryDecomposer();
        this.queryIndex = new QueryIndex(configuration, presearcher);

        long purgeFrequency = configuration.getPurgeFrequency();
        this.purgeExecutor = Executors.newSingleThreadScheduledExecutor();
        this.purgeExecutor.scheduleAtFixedRate(() -> {
            try {
                purgeCache();
            } catch (Throwable e) {
                listeners.forEach(l -> l.onPurgeError(e));
            }
        }, purgeFrequency, purgeFrequency, configuration.getPurgeFrequencyUnits());
        this.commitBatchSize = configuration.getQueryUpdateBufferSize();
    }

    /**
     * Register a {@link MonitorUpdateListener} that will be notified whenever changes
     * are made to the Monitor's queryindex
     *
     * @param listener listener to register
     */
    public void addQueryIndexUpdateListener(MonitorUpdateListener listener) {
        listeners.add(listener);
    }

    /**
     * @return Statistics for the internal query index and cache
     */
    public QueryCacheStats getQueryCacheStats() {
        return new QueryCacheStats(queryIndex.numDocs(), queryIndex.cacheSize(), lastPurged);
    }

    /**
     * Remove unused queries from the query cache.
     * <p>
     * This is normally called from a background thread at a rate set by configurePurgeFrequency().
     *
     * @throws IOException on IO errors
     */
    public void purgeCache() throws IOException {
        queryIndex.purgeCache();
        lastPurged = System.nanoTime();
        listeners.forEach(MonitorUpdateListener::onPurge);
    }

    @Override
    public void close() throws IOException {
        purgeExecutor.shutdown();
        queryIndex.close();
    }

    /**
     * Add new queries to the monitor
     *
     * @param queries the MonitorQueries to add
     */
    public void register(Iterable<MonitorQuery> queries) throws IOException {
        List<MonitorQuery> updates = new ArrayList<>();
        for (MonitorQuery query : queries) {
            updates.add(query);
            if (updates.size() > commitBatchSize) {
                commit(updates);
                updates.clear();
            }
        }
        commit(updates);
    }

    private void commit(List<MonitorQuery> updates) throws IOException {
        queryIndex.commit(updates);
        listeners.forEach(l -> l.afterUpdate(updates));
    }

    /**
     * Add new queries to the monitor
     *
     * @param queries the MonitorQueries to add
     * @throws IOException on IO errors
     */
    public void register(MonitorQuery... queries) throws IOException {
        register(Arrays.asList(queries));
    }

    /**
     * Delete queries from the monitor by ID
     *
     * @param queryIds the IDs to delete
     * @throws IOException on IO errors
     */
    public void deleteById(List<String> queryIds) throws IOException {
        queryIndex.deleteQueries(queryIds);
        listeners.forEach(l -> l.afterDelete(queryIds));
    }

    /**
     * Delete queries from the monitor by ID
     *
     * @param queryIds the IDs to delete
     * @throws IOException on IO errors
     */
    public void deleteById(String... queryIds) throws IOException {
        deleteById(Arrays.asList(queryIds));
    }

    /**
     * Delete all queries from the monitor
     *
     * @throws IOException on IO errors
     */
    public void clear() throws IOException {
        queryIndex.clear();
        listeners.forEach(MonitorUpdateListener::afterClear);
    }

    public void scanQueries(BiConsumer<String, QueryCacheEntry> consumer) throws IOException {
        queryIndex.scan(new QueryIndex.QueryCollector() {

            @Override
            public void matchQuery(String id, QueryCacheEntry query, QueryIndex.DataValues dataValues) throws IOException {
                consumer.accept(id, query);
            }

            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
            }
        });
    }

    public void search(DocumentBatch documents, QueryIndex.QueryCollector queryCollector) throws IOException {
        queryIndex.search(new PreSearcherQueryBuilder(documents.get()), queryCollector);
    }

    private class PreSearcherQueryBuilder implements QueryIndex.QueryBuilder {

        final LeafReader leafReader;

        private PreSearcherQueryBuilder(LeafReader leafReader) {
            this.leafReader = leafReader;
        }

        @Override
        public Query buildQuery(BiPredicate<String, BytesRef> termAcceptor) throws IOException {
            return presearcher.buildQuery(leafReader, termAcceptor);
        }
    }
}
