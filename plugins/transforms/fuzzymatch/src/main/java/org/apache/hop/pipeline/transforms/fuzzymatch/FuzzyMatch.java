/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.fuzzymatch;

import static org.apache.hop.pipeline.transforms.fuzzymatch.FuzzyMatchMeta.Algorithm;
import static org.apache.hop.pipeline.transforms.fuzzymatch.FuzzyMatchMeta.FMLookupValue;
import static org.apache.hop.pipeline.transforms.fuzzymatch.FuzzyMatchMeta.MatchMode;

import com.wcohen.ss.Jaro;
import com.wcohen.ss.JaroWinkler;
import com.wcohen.ss.NeedlemanWunsch;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.codec.language.Metaphone;
import org.apache.commons.codec.language.RefinedSoundex;
import org.apache.commons.codec.language.Soundex;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Performs a fuzzy match for each main stream field row An approximative match is done in a lookup
 * stream
 */
public class FuzzyMatch extends BaseTransform<FuzzyMatchMeta, FuzzyMatchData> {
  private static final Class<?> PKG = FuzzyMatchMeta.class;

  public FuzzyMatch(
      TransformMeta transformMeta,
      FuzzyMatchMeta meta,
      FuzzyMatchData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private boolean readLookupValues() throws HopException {
    data.infoStream = meta.getTransformIOMeta().getInfoStreams().get(0);
    if (data.infoStream.getTransformMeta() == null) {
      logError(BaseMessages.getString(PKG, "FuzzyMatch.Log.NoLookupTransformSpecified"));
      return false;
    }

    if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(PKG, "FuzzyMatch.Log.ReadingFromStream")
              + data.infoStream.getTransformName()
              + "]");
    }

    boolean firstRun = true;
    // Which row set do we read from?
    //
    IRowSet rowSet = findInputRowSet(data.infoStream.getTransformName());
    Object[] rowData = getRowFrom(rowSet); // rows are originating from "lookup_from"

    while (rowData != null) {
      if (firstRun) {
        data.infoMeta = rowSet.getRowMeta().clone();
        // Check lookup field
        int indexOfLookupField = data.infoMeta.indexOfValue(resolve(meta.getLookupField()));
        if (indexOfLookupField < 0) {
          // The field is unreachable !
          throw new HopException(
              BaseMessages.getString(
                  PKG, "FuzzyMatch.Exception.CouldnotFindLookField", meta.getLookupField()));
        }
        data.infoCache = new RowMeta();
        IValueMeta keyValueMeta = data.infoMeta.getValueMeta(indexOfLookupField);
        keyValueMeta.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
        data.infoCache.addValueMeta(keyValueMeta);
        // Add key
        data.indexOfCachedFields[0] = indexOfLookupField;

        // Check additional fields
        if (data.addAdditionalFields) {
          IValueMeta additionalFieldValueMeta;
          for (int i = 0; i < meta.getLookupValues().size(); i++) {
            FMLookupValue lookupValue = meta.getLookupValues().get(i);
            int fi = i + 1;
            data.indexOfCachedFields[fi] = data.infoMeta.indexOfValue(lookupValue.getName());
            if (data.indexOfCachedFields[fi] < 0) {
              // The field is unreachable !
              throw new HopException(
                  BaseMessages.getString(
                      PKG, "FuzzyMatch.Exception.CouldnotFindLookField", lookupValue.getName()));
            }
            additionalFieldValueMeta = data.infoMeta.getValueMeta(data.indexOfCachedFields[fi]);
            additionalFieldValueMeta.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
            data.infoCache.addValueMeta(additionalFieldValueMeta);
          }
          data.nrCachedFields += meta.getLookupValues().size();
        }
      }
      if (isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(PKG, "FuzzyMatch.Log.ReadLookupRow")
                + rowSet.getRowMeta().getString(rowData));
      }

      // Look up the keys in the source rows
      // and store values in cache

      Object[] storeData = new Object[data.nrCachedFields];
      // Add key field
      if (rowData[data.indexOfCachedFields[0]] == null) {
        storeData[0] = "";
      } else {
        IValueMeta fromStreamRowMeta =
            rowSet.getRowMeta().getValueMeta(data.indexOfCachedFields[0]);
        if (fromStreamRowMeta.isStorageBinaryString()) {
          storeData[0] =
              fromStreamRowMeta.convertToNormalStorageType(rowData[data.indexOfCachedFields[0]]);
        } else {
          storeData[0] = rowData[data.indexOfCachedFields[0]];
        }
      }

      // Add additional fields?
      for (int i = 1; i < data.nrCachedFields; i++) {
        IValueMeta fromStreamRowMeta =
            rowSet.getRowMeta().getValueMeta(data.indexOfCachedFields[i]);
        if (fromStreamRowMeta.isStorageBinaryString()) {
          storeData[i] =
              fromStreamRowMeta.convertToNormalStorageType(rowData[data.indexOfCachedFields[i]]);
        } else {
          storeData[i] = rowData[data.indexOfCachedFields[i]];
        }
      }
      if (isDebug()) {
        logDebug(
            BaseMessages.getString(
                PKG, "FuzzyMatch.Log.AddingValueToCache", data.infoCache.getString(storeData)));
      }

      addToCache(storeData);

      rowData = getRowFrom(rowSet);

      if (firstRun) {
        firstRun = false;
      }
    }

    return true;
  }

  private List<Object[]> lookupValues(IRowMeta rowMeta, Object[] row) throws HopException {
    if (first) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(
          data.outputRowMeta,
          getTransformName(),
          new IRowMeta[] {data.infoMeta},
          null,
          this,
          metadataProvider);

      // Check lookup field
      data.indexOfMainField = getInputRowMeta().indexOfValue(resolve(meta.getMainStreamField()));
      if (data.indexOfMainField < 0) {
        // The field is unreachable !
        throw new HopException(
            BaseMessages.getString(
                PKG, "FuzzyMatch.Exception.CouldnotFindMainField", meta.getMainStreamField()));
      }
    }
    if (row[data.indexOfMainField] == null) {
      List<Object[]> empty = new ArrayList<>(1);
      empty.add(RowDataUtil.allocateRowData(data.outputRowMeta.size()));
      return empty;
    }
    try {
      return getFromCache(row);
    } catch (Exception e) {
      throw new HopTransformException(e);
    }
  }

  private void addToCache(Object[] value) throws HopException {
    try {
      data.look.add(value);
    } catch (OutOfMemoryError o) {
      // exception out of memory
      throw new HopException(
          BaseMessages.getString(PKG, "FuzzyMatch.Error.JavaHeap", o.toString()));
    }
  }

  private List<Object[]> getFromCache(Object[] keyRow) throws HopValueException {
    if (isDebug()) {
      logDebug(
          BaseMessages.getString(
              PKG, "FuzzyMatch.Log.ReadingMainStreamRow", getInputRowMeta().getString(keyRow)));
    }
    return switch (meta.getAlgorithm()) {
      case LEVENSHTEIN, DAMERAU_LEVENSHTEIN, NEEDLEMAN_WUNSH -> doDistance(keyRow);
      case DOUBLE_METAPHONE, METAPHONE, SOUNDEX, REFINED_SOUNDEX -> doPhonetic(keyRow);
      case JARO, JARO_WINKLER, PAIR_SIMILARITY -> doSimilarity(keyRow);
      default -> List.<Object[]>of(RowDataUtil.allocateRowData(data.outputRowMeta.size()));
    };
  }

  private List<Object[]> doDistance(Object[] row) throws HopValueException {
    boolean lowerIsBetter = true;
    PriorityQueue<ScoredMatch> topK = newTopKHeap(lowerIsBetter);
    String lookupValueString = getInputRowMeta().getString(row, data.indexOfMainField);

    for (Object[] cachedData : data.look) {
      String cacheValue = (String) cachedData[0];

      String useCacheValue = cacheValue;
      String useLookupvalue = lookupValueString;
      if (!meta.isCaseSensitive()) {
        useCacheValue = cacheValue.toLowerCase();
        useLookupvalue = lookupValueString.toLowerCase();
      }

      int cDistance =
          switch (meta.getAlgorithm()) {
            case DAMERAU_LEVENSHTEIN ->
                Utils.getDamerauLevenshteinDistance(useCacheValue, useLookupvalue);
            case NEEDLEMAN_WUNSH ->
                Math.abs((int) new NeedlemanWunsch().score(useCacheValue, useLookupvalue));
            default -> StringUtils.getLevenshteinDistance(useCacheValue, useLookupvalue);
          };

      if (data.minimalDistance <= cDistance && cDistance <= data.maximalDistance) {
        offerTopK(topK, new ScoredMatch(cachedData, cDistance, (long) cDistance), lowerIsBetter);
      }
    }

    return buildMatchResults(topK, lowerIsBetter);
  }

  private List<Object[]> doPhonetic(Object[] row) {
    boolean lowerIsBetter = true; // equal scores; first-seen kept by heap capacity
    PriorityQueue<ScoredMatch> topK = newTopKHeap(lowerIsBetter);

    Object o = row[data.indexOfMainField];
    String lookupvalue = (String) o;
    String lookupValueMF = getEncodedMF(lookupvalue, meta.getAlgorithm());

    for (Object[] cachedData : data.look) {
      String cacheValue = (String) cachedData[0];
      String cacheValueMF = getEncodedMF(cacheValue, meta.getAlgorithm());
      if (lookupValueMF.equals(cacheValueMF)) {
        offerTopK(topK, new ScoredMatch(cachedData, 0, cacheValueMF), lowerIsBetter);
      }
    }

    if (meta.isCloserValue()) {
      // Preserve legacy behaviour: last matching phonetic wins
      Object[] rowData = RowDataUtil.allocateRowData(data.outputRowMeta.size());
      for (Object[] cachedData : data.look) {
        String cacheValue = (String) cachedData[0];
        String cacheValueMF = getEncodedMF(cacheValue, meta.getAlgorithm());
        if (lookupValueMF.equals(cacheValueMF)) {
          fillMatchRow(rowData, cachedData, cacheValueMF);
        }
      }
      return List.<Object[]>of(rowData);
    }

    return buildMatchResults(topK, lowerIsBetter);
  }

  private String getEncodedMF(String value, Algorithm algorithmType) {
    String encodedValueMF = "";
    switch (algorithmType) {
      case METAPHONE:
        encodedValueMF = (new Metaphone()).metaphone(value);
        break;
      case DOUBLE_METAPHONE:
        encodedValueMF = ((new DoubleMetaphone()).doubleMetaphone(value));
        break;
      case SOUNDEX:
        encodedValueMF = (new Soundex()).encode(value);
        break;
      case REFINED_SOUNDEX:
        encodedValueMF = (new RefinedSoundex()).encode(value);
        break;
      default:
        break;
    }
    return encodedValueMF;
  }

  private List<Object[]> doSimilarity(Object[] row) {
    boolean lowerIsBetter = false;
    PriorityQueue<ScoredMatch> topK = newTopKHeap(lowerIsBetter);

    Object o = row[data.indexOfMainField];
    String lookupValueString = o == null ? "" : (String) o;

    for (Object[] cachedData : data.look) {
      String cacheValue = (String) cachedData[0];

      double cSimilarity =
          switch (meta.getAlgorithm()) {
            case JARO -> new Jaro().score(cacheValue, lookupValueString);
            case JARO_WINKLER -> new JaroWinkler().score(cacheValue, lookupValueString);
            default -> LetterPairSimilarity.getSimilarity(cacheValue, lookupValueString);
          };

      if (data.minimalSimilarity <= cSimilarity && cSimilarity <= data.maximalSimilarity) {
        // Exact empty-string edge case from legacy closer-value logic
        double score = cSimilarity;
        if (cSimilarity == 0 && cacheValue.equals(lookupValueString)) {
          score = 0;
        }
        offerTopK(topK, new ScoredMatch(cachedData, score, cSimilarity), lowerIsBetter);
      }
    }

    return buildMatchResults(topK, lowerIsBetter);
  }

  private PriorityQueue<ScoredMatch> newTopKHeap(boolean lowerIsBetter) {
    // Worst candidate at the head so it can be evicted.
    Comparator<ScoredMatch> worstFirst =
        lowerIsBetter
            ? Comparator.comparingDouble((ScoredMatch m) -> m.score).reversed()
            : Comparator.comparingDouble(m -> m.score);
    return new PriorityQueue<>(Math.max(1, data.maxMatches), worstFirst);
  }

  private void offerTopK(
      PriorityQueue<ScoredMatch> heap, ScoredMatch candidate, boolean lowerIsBetter) {
    int k = meta.isCloserValue() ? 1 : data.maxMatches;
    if (k <= 0) {
      return;
    }
    if (heap.size() < k) {
      heap.offer(candidate);
      return;
    }
    ScoredMatch worst = heap.peek();
    if (worst == null) {
      return;
    }
    boolean better = lowerIsBetter ? candidate.score < worst.score : candidate.score > worst.score;
    if (better) {
      heap.poll();
      heap.offer(candidate);
    }
  }

  private List<Object[]> buildMatchResults(PriorityQueue<ScoredMatch> heap, boolean lowerIsBetter) {
    if (heap.isEmpty()) {
      return List.<Object[]>of(RowDataUtil.allocateRowData(data.outputRowMeta.size()));
    }

    List<ScoredMatch> ranked = new ArrayList<>(heap);
    ranked.sort(
        lowerIsBetter
            ? Comparator.comparingDouble(m -> m.score)
            : Comparator.comparingDouble((ScoredMatch m) -> m.score).reversed());

    MatchMode mode = meta.getMatchMode();
    if (mode == MatchMode.ALL_CONCAT) {
      return List.<Object[]>of(buildConcatRow(ranked));
    }

    // CLOSEST (k=1) and ALL_ROWS: one output addition per match
    List<Object[]> results = new ArrayList<>(ranked.size());
    for (ScoredMatch match : ranked) {
      Object[] rowData = RowDataUtil.allocateRowData(data.outputRowMeta.size());
      fillMatchRow(rowData, match.cachedData, match.measureValue);
      results.add(rowData);
    }
    return results;
  }

  private Object[] buildConcatRow(List<ScoredMatch> ranked) {
    Object[] rowData = RowDataUtil.allocateRowData(data.outputRowMeta.size());
    StringBuilder matches = new StringBuilder();
    StringBuilder measures = new StringBuilder();
    for (int i = 0; i < ranked.size(); i++) {
      ScoredMatch match = ranked.get(i);
      if (i > 0) {
        matches.append(data.valueSeparator);
        if (data.addValueFieldName) {
          measures.append(data.valueSeparator);
        }
      }
      matches.append(match.cachedData[0]);
      if (data.addValueFieldName) {
        measures.append(match.measureValue);
      }
    }
    rowData[0] = matches.toString();
    if (data.addValueFieldName) {
      rowData[1] = measures.toString();
    }
    return rowData;
  }

  private void fillMatchRow(Object[] rowData, Object[] cachedData, Object measureValue) {
    int index = 0;
    rowData[index++] = cachedData[0];
    if (data.addValueFieldName) {
      rowData[index++] = measureValue;
    }
    if (data.addAdditionalFields) {
      for (int i = 0; i < meta.getLookupValues().size(); i++) {
        rowData[index + i] = cachedData[i + 1];
      }
    }
  }

  @Override
  public boolean processRow() throws HopException {
    if (data.readLookupValues) {
      data.readLookupValues = false;

      // Read values from lookup transform (look)
      if (!readLookupValues()) {
        logError(BaseMessages.getString(PKG, "FuzzyMatch.Log.UnableToReadDataFromLookupStream"));
        setErrors(1);
        stopAll();
        return false;
      }
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "FuzzyMatch.Log.ReadValuesInMemory", data.look.size()));
      }
    }

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if (r == null) {
      // no more input to be expected...
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG, "FuzzyMatch.Log.StoppedProcessingWithEmpty", getLinesRead()));
      }
      setOutputDone();
      return false;
    }

    try {
      List<Object[]> additions = lookupValues(getInputRowMeta(), r);
      int inputSize = getInputRowMeta().size();
      for (Object[] add : additions) {
        Object[] outputRow = RowDataUtil.addRowData(Arrays.copyOf(r, r.length), inputSize, add);
        putRow(data.outputRowMeta, outputRow);
      }

      if (checkFeedback(getLinesRead()) && isBasic()) {
        logBasic(BaseMessages.getString(PKG, "FuzzyMatch.Log.LineNumber") + getLinesRead());
      }
    } catch (HopException e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        // Send this row to the error handling transform
        putError(getInputRowMeta(), r, 1, e.toString(), meta.getMainStreamField(), "FuzzyMatch001");
      } else {
        logError(
            BaseMessages.getString(PKG, "FuzzyMatch.Log.ErrorInTransformRunning") + e.getMessage());
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }

    // Check lookup and main stream field
    if (StringUtils.isEmpty(meta.getMainStreamField())) {
      logError(BaseMessages.getString(PKG, "FuzzyMatch.Error.MainStreamFieldMissing"));
      return false;
    }
    if (StringUtils.isEmpty(meta.getLookupField())) {
      logError(BaseMessages.getString(PKG, "FuzzyMatch.Error.LookupStreamFieldMissing"));
      return false;
    }

    // Checks output fields
    String matchField = resolve(meta.getOutputMatchField());
    if (StringUtils.isEmpty(matchField)) {
      logError(BaseMessages.getString(PKG, "FuzzyMatch.Error.OutputMatchFieldMissing"));
      return false;
    }

    // Metrics (distance, similarity, ...) when an output field name is provided
    data.addValueFieldName = StringUtils.isNotEmpty(resolve(meta.getOutputValueField()));

    // Set the number of fields to cache
    // default value is one
    //
    int nrFields = 1;

    if (!meta.getLookupValues().isEmpty() && meta.supportsAdditionalFields()) {
      // cache also additional fields
      data.addAdditionalFields = true;
      nrFields += meta.getLookupValues().size();
    }
    data.indexOfCachedFields = new int[nrFields];

    // Top-K size: default 10, hard-capped at 100
    int maxMatches = Const.toInt(resolve(meta.getMaxMatches()), FuzzyMatchMeta.DEFAULT_MAX_MATCHES);
    if (maxMatches < 1) {
      maxMatches = 1;
    }
    if (maxMatches > FuzzyMatchMeta.HARD_MAX_MATCHES) {
      if (isBasic()) {
        logBasic(
            BaseMessages.getString(
                PKG,
                "FuzzyMatch.Log.MaxMatchesCapped",
                maxMatches,
                FuzzyMatchMeta.HARD_MAX_MATCHES));
      }
      maxMatches = FuzzyMatchMeta.HARD_MAX_MATCHES;
    }
    data.maxMatches = maxMatches;
    if (isDetailed() && !meta.isCloserValue()) {
      logDetailed(BaseMessages.getString(PKG, "FuzzyMatch.Log.MaxMatches", data.maxMatches));
    }

    switch (meta.getAlgorithm()) {
      case LEVENSHTEIN, DAMERAU_LEVENSHTEIN, NEEDLEMAN_WUNSH:
        data.minimalDistance = Const.toInt(resolve(meta.getMinimalValue()), 0);
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "FuzzyMatch.Log.MinimalDistance", data.minimalDistance));
        }
        data.maximalDistance = Const.toInt(resolve(meta.getMaximalValue()), 5);
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "FuzzyMatch.Log.MaximalDistance", data.maximalDistance));
        }
        if (meta.isAllConcatMode()) {
          data.valueSeparator = resolve(meta.getSeparator());
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(PKG, "FuzzyMatch.Log.Separator", data.valueSeparator));
          }
        }
        break;
      case JARO, JARO_WINKLER, PAIR_SIMILARITY:
        data.minimalSimilarity = Const.toDouble(resolve(meta.getMinimalValue()), 0);
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "FuzzyMatch.Log.MinimalSimilarity", data.minimalSimilarity));
        }
        data.maximalSimilarity = Const.toDouble(resolve(meta.getMaximalValue()), 1);
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "FuzzyMatch.Log.MaximalSimilarity", data.maximalSimilarity));
        }
        if (meta.isAllConcatMode()) {
          data.valueSeparator = resolve(meta.getSeparator());
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(PKG, "FuzzyMatch.Log.Separator", data.valueSeparator));
          }
        }
        break;
      default:
        break;
    }

    data.readLookupValues = true;

    return true;
  }

  @Override
  public void dispose() {
    data.look.clear();
    super.dispose();
  }

  private static final class ScoredMatch {
    private final Object[] cachedData;
    private final double score;
    private final Object measureValue;

    private ScoredMatch(Object[] cachedData, double score, Object measureValue) {
      this.cachedData = cachedData;
      this.score = score;
      this.measureValue = measureValue;
    }
  }
}
