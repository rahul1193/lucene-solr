package org.apache.lucene.analysis.combo.queryparser;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.combo.ComboAnalyzerWrapper;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.FastCharStream;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.TokenMgrError;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

import java.io.StringReader;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MultiFieldComboQueryParser extends QueryParser {

    private static final Pattern WILDCARD_PATTERN = Pattern.compile("(\\\\.)|([?*]+)");

    private final String[] fields;
    private Map<String, List<String>> extraAnalyzerPrefixesPerField;
    private Supplier<BooleanQuery.Builder> booleanQueryBuilder = BooleanQuery.Builder::new;

    public MultiFieldComboQueryParser(String[] fields, PerFieldAnalyzerWrapper analyzer) {
        super(null, analyzer);
        this.fields = fields;
    }

    public MultiFieldComboQueryParser extraAnalyzerPrefixesPerField(Map<String, List<String>> extraAnalyzerPrefixesPerField) {
        this.extraAnalyzerPrefixesPerField = extraAnalyzerPrefixesPerField;
        return this;
    }

    public MultiFieldComboQueryParser booleanQueryBuilder(Supplier<BooleanQuery.Builder> booleanQueryBuilder) {
        this.booleanQueryBuilder = booleanQueryBuilder;
        return this;
    }

    @Override
    public Query parse(String query) throws ParseException {
        this.ReInit(new FastCharStream(new StringReader(query)));
        ParseException parseException;
        try {
            Query res = this.TopLevelQuery(this.field);
            return res != null ? res : new MatchNoDocsQuery("No query created for query : " + query);
        } catch (TokenMgrError | ParseException eX) {
            parseException = new ParseException("Cannot parse '" + query + "': " + eX.getMessage());
            parseException.initCause(eX);
            throw parseException;
        } catch (IndexSearcher.TooManyClauses eX) {
            parseException = new ParseException("Cannot parse '" + query + "': too many boolean clauses");
            parseException.initCause(eX);
            throw parseException;
        }
    }

    @Override
    protected Query getBooleanQuery(List<BooleanClause> clauses) throws ParseException {
        Query q = super.getBooleanQuery(clauses);
        if (q == null) {
            return null;
        }
        return fixNegativeQueryIfNeeded(q);
    }

    @Override
    protected Query createFieldQuery(Analyzer analyzer, BooleanClause.Occur operator, String field, String queryText, boolean quoted, int phraseSlop) {
        assert analyzer instanceof PerFieldAnalyzerWrapper;
        assert operator == BooleanClause.Occur.SHOULD || operator == BooleanClause.Occur.MUST;
        String[] fields = multiFields(field);
        if (fields.length == 1) {
            return createFieldQuerySingle((PerFieldAnalyzerWrapper) analyzer, operator, fields[0], queryText, quoted, phraseSlop);
        } else {
            boolean queryAdded = false;
            BooleanQuery.Builder builder = booleanQueryBuilder.get();
            for (String mField : fields) {
                Query queryForField = createFieldQuerySingle((PerFieldAnalyzerWrapper) analyzer, operator, mField, queryText, quoted, phraseSlop);
                if (queryForField != null) {
                    queryAdded = true;
                    builder.add(queryForField, BooleanClause.Occur.SHOULD);
                }
            }
            if (!queryAdded) {
                return null;
            }
            builder.setMinimumNumberShouldMatch(1);
            return builder.build();
        }
    }

    @Override
    protected Query getFieldQuery(String dummyField, String queryText, int slop) throws ParseException {
        if (fields.length == 1) {
            return createFieldQuery(queryText, slop, fields[0]);
        }
        BooleanQuery.Builder builder = booleanQueryBuilder.get();
        boolean queryAdded = false;
        for (String mField : fields) {
            Query fieldQuery = createFieldQuery(queryText, slop, mField);
            if (fieldQuery != null) {
                queryAdded = true;
                builder.add(fieldQuery, BooleanClause.Occur.SHOULD);
            }
        }
        if (!queryAdded) {
            return null;
        }
        builder.setMinimumNumberShouldMatch(1);
        return builder.build();
    }

    @Override
    protected Query getPrefixQuery(String dummyField, String termStr) throws ParseException {
        if (!this.getAllowLeadingWildcard() && termStr.startsWith("*")) {
            throw new ParseException("'*' not allowed as first character in PrefixQuery");
        } else {
            if (fields.length == 1) {
                return createPrefixQuery(termStr, fields[0]);
            }
            BooleanQuery.Builder builder = booleanQueryBuilder.get();
            boolean queryAdded = false;
            for (String mField : fields) {
                builder.add(createPrefixQuery(termStr, mField), BooleanClause.Occur.SHOULD);
                queryAdded = true;
            }
            if (!queryAdded) {
                return null;
            }
            builder.setMinimumNumberShouldMatch(1);
            return builder.build();
        }
    }

    @Override
    protected Query getWildcardQuery(String dummyField, String termStr) throws ParseException {
        if (fields.length == 1) {
            return createWildCardQuery(termStr, fields[0]);
        }
        BooleanQuery.Builder builder = booleanQueryBuilder.get();
        boolean queryAdded = false;
        for (String mField : fields) {
            if ("*".equals(mField) && "*".equals(termStr)) {
                return this.newMatchAllDocsQuery();
            } else {
                Query wildCardQuery = createWildCardQuery(termStr, mField);
                builder.add(wildCardQuery, BooleanClause.Occur.SHOULD);
                queryAdded = true;
            }
        }
        if (!queryAdded) {
            return null;
        }
        builder.setMinimumNumberShouldMatch(1);
        return builder.build();
    }

    @Override
    protected Query getRegexpQuery(String dummyField, String termStr) throws ParseException {
        if (fields.length == 1) {
            String mField = fields[0];
            Term t = new Term(mField, termStr);
            return this.newRegexpQuery(t);
        }
        BooleanQuery.Builder builder = booleanQueryBuilder.get();
        boolean queryAdded = false;
        for (String mField : fields) {
            Term t = new Term(mField, termStr);
            builder.add(this.newRegexpQuery(t), BooleanClause.Occur.SHOULD);
            queryAdded = true;
        }
        if (!queryAdded) {
            return null;
        }
        builder.setMinimumNumberShouldMatch(1);
        return builder.build();
    }

    @Override
    protected Query getFuzzyQuery(String dummyField, String termStr, float minSimilarity) throws ParseException {
        if (fields.length == 1) {
            return createFuzzyQuery(termStr, minSimilarity, fields[0]);
        }
        BooleanQuery.Builder builder = booleanQueryBuilder.get();
        boolean queryAdded = false;
        for (String mField : fields) {
            builder.add(createFuzzyQuery(termStr, minSimilarity, mField), BooleanClause.Occur.SHOULD);
            queryAdded = true;
        }
        if (!queryAdded) {
            return null;
        }
        builder.setMinimumNumberShouldMatch(1);
        return builder.build();
    }


    private Query createFieldQuery(String queryText, int slop, String mField) throws ParseException {
        Query fieldQuery = super.getFieldQuery(mField, queryText, true);
        fieldQuery = applySlop(fieldQuery, slop);
        return fieldQuery;
    }

    private Query createPrefixQuery(String termStr, String mField) {
        Analyzer wrappedAnalyzer = getAnalyzer(mField);
        BytesRef term = wrappedAnalyzer.normalize(mField, termStr);
        Term t = new Term(mField, term);
        return this.newPrefixQuery(t);
    }

    private Query createWildCardQuery(String termStr, String mField) throws ParseException {
        if ("*".equals(mField) && "*".equals(termStr)) {
            return this.newMatchAllDocsQuery();
        } else if (this.getAllowLeadingWildcard() || !termStr.startsWith("*") && !termStr.startsWith("?")) {
            Term t = new Term(mField, analyzeWildcard(mField, termStr));
            return this.newWildcardQuery(t);
        } else {
            throw new ParseException("'*' or '?' not allowed as first character in WildcardQuery");
        }
    }

    private BytesRef analyzeWildcard(String field, String termStr) {
        Matcher wildcardMatcher = WILDCARD_PATTERN.matcher(termStr);
        BytesRefBuilder sb = new BytesRefBuilder();

        int last;
        String chunk;
        BytesRef normalized;
        for (last = 0; wildcardMatcher.find(); last = wildcardMatcher.end()) {
            if (wildcardMatcher.start() > 0) {
                chunk = termStr.substring(last, wildcardMatcher.start());
                normalized = getAnalyzer(field).normalize(field, chunk);
                sb.append(normalized);
            }

            sb.append(new BytesRef(wildcardMatcher.group()));
        }

        if (last < termStr.length()) {
            chunk = termStr.substring(last);
            normalized = getAnalyzer(field).normalize(field, chunk);
            sb.append(normalized);
        }

        return sb.toBytesRef();
    }

    private Query createFuzzyQuery(String termStr, float minSimilarity, String field) {
        BytesRef term = getAnalyzer().normalize(field, termStr);
        Term t = new Term(field, term);
        return this.newFuzzyQuery(t, minSimilarity, this.getFuzzyPrefixLength());
    }

    private Query createFieldQuerySingle(PerFieldAnalyzerWrapper perFieldAnalyzerWrapper, BooleanClause.Occur operator, String field, String queryText, boolean quoted, int phraseSlop) {
        Analyzer analyzerForField = perFieldAnalyzerWrapper.getWrappedAnalyzer(field);
        if (analyzerForField == null) {
            throw new RuntimeException("No Analyzer found for field : " + field);
        }
        Query queryForField = null;
        if (analyzerForField instanceof ComboAnalyzerWrapper) {
            ComboAnalyzerWrapper comboAnalyzerWrapper = (ComboAnalyzerWrapper) analyzerForField;
            Set<String> analyzerNames = new HashSet<>(comboAnalyzerWrapper.getSubAnalyzerNames());
            if (extraAnalyzerPrefixesPerField != null) {
                List<String> analyzerPrefixes = extraAnalyzerPrefixesPerField.get(field);
                if (analyzerPrefixes != null) {
                    for (String prefix : analyzerPrefixes) {
                        analyzerNames.addAll(comboAnalyzerWrapper.getAnalyzerNames(prefix));
                    }
                }
            }
            Set<Query> deDupedQueries = new HashSet<>();
            for (String analyzerName : analyzerNames) {
                Analyzer analyzer = comboAnalyzerWrapper.getAnalyzer(analyzerName);
                if (analyzer != null) {
                    Query fieldQuery = super.createFieldQuery(analyzer, operator, field, queryText, quoted, phraseSlop);
                    if (fieldQuery != null) {
                        deDupedQueries.add(fieldQuery);
                    }
                }
            }
            if (!deDupedQueries.isEmpty()) {
                BooleanQuery.Builder builder = booleanQueryBuilder.get();
                deDupedQueries.forEach(fieldQuery -> builder.add(fieldQuery, BooleanClause.Occur.SHOULD));
                queryForField = builder.setMinimumNumberShouldMatch(1).build();
            }
        } else {
            queryForField = super.createFieldQuery(analyzerForField, operator, field, queryText, quoted, phraseSlop);
        }
        return queryForField;
    }

    private Analyzer getAnalyzer(String mField) {
        Analyzer analyzer = getAnalyzer();
        assert analyzer instanceof PerFieldAnalyzerWrapper;
        Analyzer wrappedAnalyzer = ((PerFieldAnalyzerWrapper) analyzer).getWrappedAnalyzer(mField);
        return wrappedAnalyzer;
    }

    private Query applySlop(Query query, int slop) {
        if (query instanceof BooleanQuery) {
            BooleanQuery.Builder builder =
                    booleanQueryBuilder.get().setMinimumNumberShouldMatch(((BooleanQuery) query).getMinimumNumberShouldMatch());
            ((BooleanQuery) query).forEach(clause -> builder.add(applySlop(clause.getQuery(), slop), clause.getOccur()));
            query = builder.build();
        } else if (query instanceof PhraseQuery) {
            query = addSlopToPhrase((PhraseQuery) query, slop);
        } else if (query instanceof MultiPhraseQuery) {
            MultiPhraseQuery mpq = (MultiPhraseQuery) query;
            if (slop != mpq.getSlop()) {
                query = (new MultiPhraseQuery.Builder(mpq)).setSlop(slop).build();
            }
        }
        return query;
    }

    private PhraseQuery addSlopToPhrase(PhraseQuery query, int slop) {
        PhraseQuery.Builder builder = new PhraseQuery.Builder();
        builder.setSlop(slop);
        Term[] terms = query.getTerms();
        int[] positions = query.getPositions();
        for (int i = 0; i < terms.length; ++i) {
            builder.add(terms[i], positions[i]);
        }
        return builder.build();
    }

    private String[] multiFields(String field) {
        if (field != null && field.length() > 0) {
            return new String[]{field};
        }
        return fields;
    }

    private Query fixNegativeQueryIfNeeded(Query q) {
        if (isNegativeQuery(q)) {
            BooleanQuery bq = (BooleanQuery) q;
            BooleanQuery.Builder builder = booleanQueryBuilder.get();
            for (BooleanClause clause : bq) {
                builder.add(clause);
            }
            builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
            return builder.build();
        }
        return q;
    }

    private static boolean isNegativeQuery(Query q) {
        if (!(q instanceof BooleanQuery)) {
            return false;
        }
        List<BooleanClause> clauses = ((BooleanQuery) q).clauses();
        if (clauses.isEmpty()) {
            return false;
        }
        for (BooleanClause clause : clauses) {
            if (!clause.isProhibited()) {
                return false;
            }
        }
        return true;
    }
}
