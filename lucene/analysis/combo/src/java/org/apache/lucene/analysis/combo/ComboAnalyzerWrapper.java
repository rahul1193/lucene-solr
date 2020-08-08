package org.apache.lucene.analysis.combo;

import org.apache.lucene.analysis.Analyzer;

import java.util.*;

public final class ComboAnalyzerWrapper extends Analyzer {

    private final List<String> subAnalyzerNames;
    private final Boolean merge;
    private final Integer positionGap;
    private final String prefix;
    private final Map<String, List<String>> prefixAnalyzerMap;
    private final Map<String, List<String>> excludeAnalyzersForPrefixMap;
    private final AnalyzerProvider analyzerProvider;

    private ComboAnalyzer analyzer;

    public ComboAnalyzerWrapper(List<String> subAnalyzerNames, Boolean merge, Integer positionGap, String prefix,
                                Map<String, List<String>> prefixAnalyzerMap, Map<String, List<String>> excludeAnalyzersForPrefixMap,
                                AnalyzerProvider analyzerProvider) {

        if (subAnalyzerNames == null || subAnalyzerNames.isEmpty()) {
            throw new IllegalArgumentException("Analyzer of type combo, must have a \"sub_analyzers\" list property");
        }
        if (analyzerProvider == null) {
            throw new IllegalArgumentException("Analyzer lookup not provided");
        }

        this.subAnalyzerNames = subAnalyzerNames;
        this.merge = merge;
        this.positionGap = positionGap;
        this.prefix = prefix;
        this.prefixAnalyzerMap = prefixAnalyzerMap;
        this.excludeAnalyzersForPrefixMap = excludeAnalyzersForPrefixMap;
        this.analyzerProvider = analyzerProvider;
    }

    /**
     * Read settings and load the appropriate sub-analyzers.
     */
    synchronized protected void init() {
        if (analyzer != null) {
            return;
        }

        final Map<String, Set<Analyzer>> prefixVsAnalyzerSet = new HashMap<>();

        Map<String, Analyzer> subAnalyzers = new LinkedHashMap<>();
        for (String subname : subAnalyzerNames) {
            Analyzer analyzer = Objects.requireNonNull(getAnalyzer(subname), subname + " sub-analyzer not found");
            subAnalyzers.put(subname, analyzer);
        }
        prefixVsAnalyzerSet.put(null, new LinkedHashSet<>(subAnalyzers.values()));

        if (prefixAnalyzerMap != null) {
            for (Map.Entry<String, List<String>> entry : prefixAnalyzerMap.entrySet()) {
                List<String> analyzersToExclude = null;
                if (excludeAnalyzersForPrefixMap != null) {
                    analyzersToExclude = excludeAnalyzersForPrefixMap.get(entry.getKey());
                }
                Map<String, Analyzer> subAnalyzersCopy = new LinkedHashMap<>(subAnalyzers);
                if (analyzersToExclude != null) {
                    for (String name : analyzersToExclude) {
                        subAnalyzersCopy.remove(name);
                    }
                }
                Set<Analyzer> analyzers = new LinkedHashSet<>(subAnalyzersCopy.values());
                for (String name : entry.getValue()) {
                    Analyzer analyzer = getAnalyzer(name);
                    if (analyzer != null) {
                        if (analyzersToExclude != null && analyzersToExclude.contains(name)) {
                            continue;
                        }
                        analyzers.add(analyzer);
                    }
                }
                prefixVsAnalyzerSet.put(entry.getKey(), analyzers);
            }
        }

        this.analyzer = new ComboAnalyzer(prefix, prefixVsAnalyzerSet);

        if (merge != null) {
            this.analyzer.merge(merge);
        }
        if (positionGap != null) {
            this.analyzer.positionGapBetweenAnalyzerTokens(positionGap);
        }
    }

    public Analyzer getAnalyzer(String name) {
        return analyzerProvider.getAnalyzer(name);
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        if (analyzer == null) {
            init();
        }
        return this.analyzer.createComponents(fieldName);
    }

    @Override
    public void close() {
        if (analyzer != null) {
            this.analyzer.close();
        }
        super.close();
    }

    public Set<String> getAnalyzerNames(String prefix) {
        if (prefixAnalyzerMap == null) {
            return Collections.emptySet();
        }
        Collection<String> analyzerNames = prefixAnalyzerMap.get(prefix);
        if (analyzerNames == null) {
            return Collections.emptySet();
        }
        analyzerNames = new HashSet<>(analyzerNames);
        if (excludeAnalyzersForPrefixMap != null && excludeAnalyzersForPrefixMap.containsKey(prefix)) {
            analyzerNames.removeAll(excludeAnalyzersForPrefixMap.get(prefix));
        }
        return new HashSet<>(analyzerNames);
    }

    @FunctionalInterface
    public interface AnalyzerProvider {
        Analyzer getAnalyzer(String name);
    }

    public List<String> getSubAnalyzerNames() {
        return subAnalyzerNames;
    }
}
