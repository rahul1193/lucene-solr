package org.apache.lucene.monitor;

/**
 * Statistics for the query cache and query index
 */
public class QueryCacheStats {

    /**
     * Total number of queries in the query index
     */
    public final int queries;

    /**
     * Total number of queries int the query cache
     */
    public final int cachedQueries;

    /**
     * Time the query cache was last purged
     */
    public final long lastPurged;

    public QueryCacheStats(int queries, int cachedQueries, long lastPurged) {
        this.queries = queries;
        this.cachedQueries = cachedQueries;
        this.lastPurged = lastPurged;
    }
}
