/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.codecs;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.codecs.lucene54.Lucene54DocValuesFormat;
import org.apache.lucene.codecs.lucene70.Lucene70Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Date: 09/10/18
 * Time: 12:32 PM
 *
 * @author abhishek.kumar
 */
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "coz why not")
@SuppressWarnings("deprecation")
public class Lucene70vs54Benchmarks extends LuceneTestCase {


  // test codecs
  private final Codec lucene70Codec = new Lucene70Codec();
  private final Codec lucene54Codec = new Lucene70Codec() {
    final DocValuesFormat dv = new Lucene54DocValuesFormat();

    @Override
    public DocValuesFormat getDocValuesFormatForField(String field) {
      return dv;
    }
  };

  // benchmark parameters
  private static final String SPARSE_DV_FIELD_NAME = "sparse";
  private static final String DENSE_DV_FIELD_NAME = "dense";
  private static final String ALL_DV_FIELD_NAME = "all";
  private static final boolean verbose = false;
  private static final boolean rewrite = false;
  private static final boolean printFieldStats = false;
  private static final int runs = 10;
  private static final int numDocs = 4000;
  private static final int maxWordsPerDoc = 1000;
  private static final boolean perfStatsEnabled = false;
  private static final String path = "/mnt1/lucene/benchmarks";
  private static PerfTracker.PerfStats stats;

  public void testBenchmark() throws IOException {
    final String[] uniqueWords = getUniqueWords(1, 1024, 1 << 15);
    deleteDirectoryIfRequired(path);
    List<Runnable> runnables = new ArrayList<>(6);
    for (String testField : new String[]{SPARSE_DV_FIELD_NAME, DENSE_DV_FIELD_NAME, ALL_DV_FIELD_NAME}) {
      runnables.add(() -> testHelper("Lucene54", lucene54Codec, path, uniqueWords, testField));
      runnables.add(() -> testHelper("Lucene70", lucene70Codec, path, uniqueWords, testField));
    }
//    Collections.shuffle(runnables);
    runnables.forEach(Runnable::run);
  }

  // runs a test for a codec
  private static void testHelper(String codecName, Codec codec, String path, String[] uniqueWords, String testField) {
    log(codecName + " Iterating over all doc values of a " + testField + " field");
    Path directoryPath = Paths.get(path, codecName, testField);
    try (Directory dir = new MMapDirectory(directoryPath)) {
      writeDocs(codec, dir, directoryPath, uniqueWords, testField);
      long size = size(directoryPath);
      log("Directory : " + dir.getClass().getSimpleName() + "@" + directoryPath.toAbsolutePath().toString() + String.format(" Size %d KB", size / 1024));
      printFieldStatsIfEnabled(dir);
      for (int i = 0; i < runs; i++) {
        readDocs("WarmUp " + i, dir, testField);
      }
      for (int i = 0; i < runs; i++) {
        readDocs("Run " + i, dir, testField);
      }
      logLine();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // reading field stats functions
  private static void printFieldStatsIfEnabled(Directory dir) throws IOException {
    if (printFieldStats) {
      logLine();
      long startTime = System.nanoTime();
      DirectoryReader ir = DirectoryReader.open(dir);
      for (LeafReaderContext leafReaderContext : ir.leaves()) {
        LeafReader r = leafReaderContext.reader();
        FieldInfos fieldInfos = r.getFieldInfos();
        for (FieldInfo fieldInfo : fieldInfos) {
          log("Field #" + fieldInfo.number + " " + fieldInfo.name + ": " + r.getDocCount(fieldInfo.name) + " " + r.getDocCount(fieldInfo.name));
          if (fieldInfo.name.equals("id")) {
            log("");
            continue;
          }
          SortedSetDocValues docValues = r.getSortedSetDocValues(fieldInfo.name);
          List<Integer> docCounts = new ArrayList<>(r.maxDoc());
          for (int i = 0; i < r.maxDoc(); i++) {
            int words = 0;
            if (docValues.advanceExact(i)) {
              long ord;
              while ((ord = docValues.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
                docValues.lookupOrd(ord);
                words++;
              }
            }
            docCounts.add(words);
          }
          log(fieldInfo.name + " stats: " + Stats.ofInts(docCounts));
          log("");
        }
      }
      logTimeTaken("field stats", startTime);
      logLine();
    }
  }

  // reading docs function
  private static void readDocs(String prefix, Directory dir, String testField) throws IOException {
    stats = PerfTracker.start();
    DirectoryReader ir = DirectoryReader.open(dir);
    long startTime = System.nanoTime();
    for (LeafReaderContext context : ir.leaves()) {
      LeafReader r = context.reader();
      stats.in("getSortedSetDocValues");
      SortedSetDocValues docValues = r.getSortedSetDocValues(testField);
      stats.out("getSortedSetDocValues");
      for (int i = 0; i < r.maxDoc(); i++) {
        stats.in("advanceExact");
        boolean advanced = docValues.advanceExact(i);
        stats.out("advanceExact");
        if (advanced) {
          long ord;
          while ((ord = getNextOrd(docValues)) != SortedSetDocValues.NO_MORE_ORDS) {
            stats.in("lookupOrd");
            docValues.lookupOrd(ord);
            stats.out("lookupOrd");
          }
        }
      }
    }
    logTimeTaken(prefix, startTime);
    plog("perfStats (ms): " + stats.stop());
  }

  private static long getNextOrd(SortedSetDocValues docValues) throws IOException {
    try {
      stats.in("nextOrd");
      return docValues.nextOrd();
    } finally {
      stats.out("nextOrd");
    }
  }

  // writing docs function
  private static void writeDocs(Codec codec, Directory dir, Path directoryPath, String[] uniqueWords, String testField) throws IOException {
    if (!rewrite && directoryPath.toFile().exists()) {
      return;
    }
    log(String.format("Writing %d Docs.. with max words per doc %d", numDocs, maxWordsPerDoc));
    long startTime = System.nanoTime();
    IndexWriterConfig conf = new IndexWriterConfig();
    conf.setCodec(codec);
    //  conf.setInfoStream(System.out);
    IndexWriter writer = new IndexWriter(dir, conf);

    FieldType dvFieldType = new FieldType();
    dvFieldType.setDocValuesType(DocValuesType.SORTED_SET);
    dvFieldType.setStored(false);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();

      // add id field
      Field idField = new StringField("id", Integer.toString(i), Field.Store.NO);
      doc.add(idField);

      int numValues = TestUtil.nextInt(random(), 0, maxWordsPerDoc);
      Set<String> values = new TreeSet<>();
      for (int v = 0; v < numValues; v++) {
        values.add(RandomPicks.randomFrom(random(), uniqueWords));
      }

      // add in any order to the dv field
      ArrayList<String> unordered = new ArrayList<>(values);
      Collections.shuffle(unordered, random());
      boolean addInSparseField = TestUtil.nextInt(random(), 0, 9) == 0;
      for (String v : unordered) {
        switch (testField) {
          case DENSE_DV_FIELD_NAME:
            doc.add(new Field(DENSE_DV_FIELD_NAME, new BytesRef(v), dvFieldType));
            break;
          case ALL_DV_FIELD_NAME:
            doc.add(new Field(ALL_DV_FIELD_NAME, new BytesRef(v), dvFieldType));
            break;
          case SPARSE_DV_FIELD_NAME:
            if (addInSparseField) {
              doc.add(new Field(SPARSE_DV_FIELD_NAME, new BytesRef(v), dvFieldType));
            }
            break;
        }
      }

      if (unordered.size() == 0 && testField.equals(ALL_DV_FIELD_NAME)) {
        doc.add(new Field(ALL_DV_FIELD_NAME, new BytesRef(RandomPicks.randomFrom(random(), uniqueWords)), dvFieldType));
      }

      writer.addDocument(doc);
    }
    writer.commit();
    writer.forceMerge(1);
    writer.close();
    logTimeTaken(startTime);
  }

  // unique words generator function
  private static String[] getUniqueWords(int minWordLength, int maxWordLength, int maxUniqueWords) {
    if (!rewrite) {
      return null;
    }
    log("Generating Unique Words..");
    long startTime = System.nanoTime();
    Set<String> valueSet = new HashSet<>();
    for (int i = 0; i < 10000 && valueSet.size() < maxUniqueWords; ++i) {
      final int length = TestUtil.nextInt(random(), minWordLength, maxWordLength);
      valueSet.add(TestUtil.randomSimpleString(random(), length));
    }
    try {
      log("" + valueSet.size() + " words generated");
      return valueSet.toArray(new String[0]);
    } finally {
      logTimeTaken(startTime);
      logLine();
    }
  }

  // logging helper functions

  private static void logTimeTaken(long startTime) {
    logTimeTaken(null, startTime);
  }

  private static void logTimeTaken(String prefix, long startTime) {
    log(String.format("%sTime Taken %d ms", prefix == null ? "" : prefix + " ", (System.nanoTime() - startTime) / 1000_000));
  }

  private static void logLine() {
    log("------------------------------------------------------");
  }

  private static void log(String msg) {
    System.out.println(msg);
  }

  private static void vlog(String msg) {
    if (verbose) {
      log(msg);
    }
  }

  private static void plog(String msg) {
    if (perfStatsEnabled) {
      log(msg);
    }
  }

  // Directory helper functions

  private static void deleteDirectoryIfRequired(String path) throws IOException {
    Path directoryPath = Paths.get(path);
    if (rewrite && directoryPath.toFile().exists()) {
      deleteDirectory(directoryPath);
      log(path + " deleted");
    }
    logLine();
  }

  public static long size(Path path) throws IOException {
    final AtomicLong size = new AtomicLong(0);
    Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
        size.addAndGet(attrs.size());
        return FileVisitResult.CONTINUE;
      }
    });
    return size.get();
  }

  private static void deleteDirectory(Path directoryPath) throws IOException {
    Files.walkFileTree(directoryPath, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return super.visitFile(file, attrs);
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return super.postVisitDirectory(dir, exc);
      }
    });
  }

  // stats generator class

  private static class Stats {

    private final List<Long> input;
    private final double mean;
    private final long min;
    private final long max;
    private final long sum;
    private final int count;
    private final long median;
    private final double variance;
    private final double stdDev;
    private final Map<Long, Long> percentiles;

    private static Stats ofInts(Collection<Integer> inputArg) {
      return new Stats(inputArg.stream().map(Long::valueOf).collect(Collectors.toList()));
    }

    private static Stats of(Collection<Long> inputArg) {
      return new Stats(inputArg);
    }

    private Stats(Collection<Long> inputArg) {
      input = new ArrayList<>(inputArg);
      Collections.sort(input);
      LongSummaryStatistics statistics = input.stream().mapToLong(i -> i).summaryStatistics();
      mean = statistics.getAverage();
      min = statistics.getMin();
      max = statistics.getMax();
      sum = statistics.getSum();
      count = (int) statistics.getCount();
      median = count % 2 != 0 ? input.get((count - 1) / 2) : (input.get(count / 2) + input.get(count / 2 - 1)) / 2;
      long sumOfSquares = input.stream().mapToLong(i -> i * i).sum();
      variance = sumOfSquares / count - Math.pow(sum / count, 2);
      stdDev = Math.sqrt(variance);
      percentiles = generatePercentiles(20);
    }

    private Map<Long, Long> generatePercentiles(int n) {
      Map<Long, Long> rv = new TreeMap<>();
      List<Long> buckets = getPercentileBuckets(n);
      for (Long bucket : buckets) {
        rv.put(bucket, getPercentile(input, bucket));
      }
      return rv;
    }

    private static Long getPercentile(List<Long> input, Long percentile) {
      return input.get(new Double(Math.ceil((input.size() * percentile) / 100) - 1).intValue());
    }

    private List<Long> getPercentileBuckets(int n) {
      final List<Long> percentiles = new ArrayList<>(n);
      final double k = 100.0 / n;
      for (int i = 0; i < n - 1; i++) {
        percentiles.add(new Double(Math.ceil(k * (i + 1))).longValue());
      }
      percentiles.add(100L);
      return percentiles;
    }

    @Override
    public String toString() {
      return "Stats{" +
          "mean=" + mean +
          ", min=" + min +
          ", max=" + max +
          ", sum=" + sum +
          ", count=" + count +
          ", median=" + median +
          ", variance=" + variance +
          ", stdDev=" + stdDev +
          ", \npercentiles=" + percentiles +
          '}';
    }
  }

  public static class PerfTracker {

    private static final ThreadLocal<PerfStats> PERF_STATS = new ThreadLocal<>();

    public static PerfStats start() {
      PERF_STATS.set(new PerfStats(PERF_STATS.get()));
      return PERF_STATS.get();
    }

    public static void in(String statName) {
      PerfStats perfStats = PERF_STATS.get();
      if (perfStats == null) {
        return;
      }
      perfStats.in(statName);
    }

    public static void out(String statName) {
      PerfStats perfStats = PERF_STATS.get();
      if (perfStats == null) {
        return;
      }
      perfStats.out(statName);
    }

    public static void track(String key, Long timeTaken) {
      PerfStats perfStats = PERF_STATS.get();
      if (perfStats == null || timeTaken == null) {
        return;
      }
      long currentTime = System.nanoTime();
      perfStats.inOut(key, currentTime - timeTaken, currentTime);
    }

    public static class PerfStats {

      private final PerfStats parent;
      private final int parentCounts;
      private Map<String, List<Long[]>> times = new LinkedHashMap<>();
      private boolean stopped = false;
      private LinkedHashMap<String, Long> stats = new LinkedHashMap<>();

      private PerfStats(PerfStats parent) {
        this.parent = parent;
        this.parentCounts = parent == null ? 0 : parent.parentCounts + 1;
      }

      public Map<String, Long> stop() {
        if (stopped || perfStatsEnabled) {
          return stats;
        }
        if (parent != null) {
          parent.times.putAll(this.times);
        }
        PERF_STATS.set(parent);
        stats = calculateStats();
        stopped = true;
        return stats;
      }

      private LinkedHashMap<String, Long> calculateStats() {
        LinkedHashMap<String, Long> stats;
        if (this.stats != null && this.stats.size() != 0) {
          stats = new LinkedHashMap<>(this.stats);
        } else {
          stats = new LinkedHashMap<>();
        }
        for (Map.Entry<String, List<Long[]>> entry : this.times.entrySet()) {
          List<Long[]> times = entry.getValue();
          Long sum = 0L;
          for (Long[] time : times) {
            if (time[1] > 0L) {
              sum += time[1] - time[0];
            }
          }
          stats.put(entry.getKey(), sum / 1000000);

        }
        return stats;
      }

      private void in(String statName) {
        if (stopped || perfStatsEnabled) {
          return;
        }
        times.computeIfAbsent(statName, k -> new ArrayList<>()).add(new Long[]{System.nanoTime(), -1L});
      }

      private void out(String statName) {
        if (stopped || perfStatsEnabled) {
          return;
        }
        long outTime = System.nanoTime();
        Long[] times = this.times.get(statName).get(this.times.get(statName).size() - 1);
        if (times != null) {
          times[1] = outTime;
        }
      }

      private void inOut(String statName, Long startTime, Long endTime) {
        if (stopped || perfStatsEnabled) {
          return;
        }
        times.computeIfAbsent(statName, k -> new ArrayList<>()).add(new Long[]{startTime, endTime});
      }
    }
  }

}
