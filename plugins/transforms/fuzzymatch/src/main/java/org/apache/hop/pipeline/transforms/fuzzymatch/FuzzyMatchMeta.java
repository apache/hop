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

import static org.apache.hop.pipeline.transforms.fuzzymatch.FuzzyMatchMeta.Algorithm.DOUBLE_METAPHONE;
import static org.apache.hop.pipeline.transforms.fuzzymatch.FuzzyMatchMeta.Algorithm.METAPHONE;
import static org.apache.hop.pipeline.transforms.fuzzymatch.FuzzyMatchMeta.Algorithm.NONE;
import static org.apache.hop.pipeline.transforms.fuzzymatch.FuzzyMatchMeta.Algorithm.REFINED_SOUNDEX;
import static org.apache.hop.pipeline.transforms.fuzzymatch.FuzzyMatchMeta.Algorithm.SOUNDEX;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.IStream.StreamType;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;

@Getter
@Setter
@Transform(
    id = "FuzzyMatch",
    image = "fuzzymatch.svg",
    name = "i18n::FuzzyMatch.Name",
    description = "i18n::FuzzyMatch.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    keywords = "i18n::FuzzyMatchMeta.keyword",
    documentationUrl = "/pipeline/transforms/fuzzymatch.html")
public class FuzzyMatchMeta extends BaseTransformMeta<FuzzyMatch, FuzzyMatchData> {
  private static final Class<?> PKG = FuzzyMatchMeta.class;

  public static final String DEFAULT_SEPARATOR = ",";
  public static final int DEFAULT_MAX_MATCHES = 10;

  /** Hard safety cap for Top-K to avoid runaway memory / row explosion. */
  public static final int HARD_MAX_MATCHES = 100;

  /** Algorithms type */
  @HopMetadataProperty(key = "algorithm", storeWithCode = true)
  private Algorithm algorithm;

  @HopMetadataProperty(key = "from")
  private String lookupTransformName;

  /** field in lookup stream with which we look up values */
  @HopMetadataProperty(key = "lookupfield")
  private String lookupField;

  /** field in input stream for which we lookup values */
  @HopMetadataProperty(key = "mainstreamfield")
  private String mainStreamField;

  /** output match fieldname */
  @HopMetadataProperty(key = "outputmatchfield")
  private String outputMatchField;

  /** ouput value fieldname */
  @HopMetadataProperty(key = "outputvaluefield")
  private String outputValueField;

  /** case sensitive */
  @HopMetadataProperty(key = "caseSensitive")
  private boolean caseSensitive;

  /** minimal value, distance for levenshtein, similarity, ... */
  @HopMetadataProperty(key = "minimalValue")
  private String minimalValue;

  /** maximal value, distance for levenshtein, similarity, ... */
  @HopMetadataProperty(key = "maximalValue")
  private String maximalValue;

  /** values separator ... */
  @HopMetadataProperty(key = "separator")
  private String separator;

  /**
   * Legacy flag kept for backward compatibility with existing pipelines. Prefer {@link #matchMode}.
   */
  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  @HopMetadataProperty(key = "closervalue")
  private boolean closerValue;

  /** How multiple matches are returned. */
  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  @HopMetadataProperty(key = "matchMode", storeWithCode = true)
  private MatchMode matchMode;

  /** Max matches for Top-K (ALL_ROWS / ALL_CONCAT). Default 10, hard-capped at 100. */
  @HopMetadataProperty(key = "maxMatches")
  private String maxMatches;

  /** return these field values from lookup */
  @HopMetadataProperty(groupKey = "lookup", key = "value")
  private List<FMLookupValue> lookupValues;

  public FuzzyMatchMeta() {
    super();
    this.algorithm = NONE;
    this.lookupValues = new ArrayList<>();
    this.matchMode = MatchMode.CLOSEST;
    this.closerValue = true;
    this.maxMatches = String.valueOf(DEFAULT_MAX_MATCHES);
  }

  public FuzzyMatchMeta(FuzzyMatchMeta m) {
    this();
    this.algorithm = m.algorithm;
    this.lookupField = m.lookupField;
    this.mainStreamField = m.mainStreamField;
    this.outputMatchField = m.outputMatchField;
    this.outputValueField = m.outputValueField;
    this.caseSensitive = m.caseSensitive;
    this.minimalValue = m.minimalValue;
    this.maximalValue = m.maximalValue;
    this.separator = m.separator;
    setMatchMode(m.getMatchMode());
    this.maxMatches = m.maxMatches;
    m.lookupValues.forEach(v -> this.lookupValues.add(new FMLookupValue(v)));
  }

  @Override
  public FuzzyMatchMeta clone() {
    return new FuzzyMatchMeta(this);
  }

  @Override
  public void setDefault() {
    algorithm = NONE;
    separator = DEFAULT_SEPARATOR;
    setMatchMode(MatchMode.CLOSEST);
    maxMatches = String.valueOf(DEFAULT_MAX_MATCHES);
    minimalValue = "0";
    maximalValue = "1";
    caseSensitive = false;
    lookupField = null;
    mainStreamField = null;
    outputMatchField = BaseMessages.getString(PKG, "FuzzyMatchMeta.OutputMatchFieldname");
    outputValueField = BaseMessages.getString(PKG, "FuzzyMatchMeta.OutputValueFieldname");
  }

  public MatchMode getMatchMode() {
    if (matchMode != null) {
      return matchMode;
    }
    // Legacy pipelines only have closer value
    return closerValue ? MatchMode.CLOSEST : MatchMode.ALL_CONCAT;
  }

  public void setMatchMode(MatchMode matchMode) {
    this.matchMode = matchMode == null ? MatchMode.CLOSEST : matchMode;
    this.closerValue = this.matchMode == MatchMode.CLOSEST;
  }

  /** use {@link #getMatchMode()} */
  public boolean isCloserValue() {
    return getMatchMode() == MatchMode.CLOSEST;
  }

  /** use {@link #setMatchMode(MatchMode)} */
  public void setCloserValue(boolean closerValue) {
    this.closerValue = closerValue;
    // When loading legacy XML, matchMode may still be unset.
    if (matchMode == null || matchMode == MatchMode.CLOSEST || matchMode == MatchMode.ALL_CONCAT) {
      this.matchMode = closerValue ? MatchMode.CLOSEST : MatchMode.ALL_CONCAT;
    }
  }

  public boolean isAllRowsMode() {
    return getMatchMode() == MatchMode.ALL_ROWS;
  }

  public boolean isAllConcatMode() {
    return getMatchMode() == MatchMode.ALL_CONCAT;
  }

  public boolean supportsAdditionalFields() {
    return isCloserValue()
        || isAllRowsMode()
        || getAlgorithm() == DOUBLE_METAPHONE
        || getAlgorithm() == SOUNDEX
        || getAlgorithm() == REFINED_SOUNDEX
        || getAlgorithm() == METAPHONE;
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Add match field
    IValueMeta v = new ValueMetaString(variables.resolve(getOutputMatchField()));
    v.setOrigin(name);
    v.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
    inputRowMeta.addValueMeta(v);

    String mainField = variables.resolve(getOutputValueField());
    if (StringUtils.isNotEmpty(mainField)) {
      if (isAllConcatMode()) {
        // Concatenated measures are always a string list
        v = new ValueMetaString(mainField);
      } else {
        switch (getAlgorithm()) {
          case NONE:
            throw new HopTransformException("Please specify the matching algorithm to use");
          case LEVENSHTEIN, DAMERAU_LEVENSHTEIN, NEEDLEMAN_WUNSH:
            // Distance algorithms return an integer measure
            v = new ValueMetaInteger(mainField);
            v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH);
            break;
          case JARO, JARO_WINKLER, PAIR_SIMILARITY:
            v = new ValueMetaNumber(mainField);
            break;
          default:
            // Phonetic algorithms
            v = new ValueMetaString(mainField);
            break;
        }
      }
      v.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }

    if (supportsAdditionalFields()) {
      if (info != null && info.length == 1 && info[0] != null) {
        for (FMLookupValue lookupValue : lookupValues) {
          v = info[0].searchValueMeta(lookupValue.getName());
          if (v != null) {
            // Configuration error/missing resources...
            v.setName(lookupValue.getName());
            v.setOrigin(name);
            // Only normal storage goes into the cache
            v.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
            replaceValueMeta(inputRowMeta, lookupValue, v);
          } else {
            throw new HopTransformException(
                BaseMessages.getString(
                    PKG,
                    "FuzzyMatchMeta.Exception.ReturnValueCanNotBeFound",
                    lookupValue.getName()));
          }
        }
      } else {
        for (FMLookupValue lookupValue : lookupValues) {
          v = new ValueMetaString(lookupValue.getName());
          v.setOrigin(name);
          replaceValueMeta(inputRowMeta, lookupValue, v);
        }
      }
    }
  }

  /**
   * Replaces or adds a value meta in the given row meta structure.
   *
   * @param inputRowMeta The row meta where the new field should be inserted.
   * @param lookupValue The lookup configuration that may contain a rename target.
   * @param newMeta The value meta to add or replace.
   */
  private void replaceValueMeta(
      IRowMeta inputRowMeta, FMLookupValue lookupValue, IValueMeta newMeta) {
    int index = inputRowMeta.indexOfValue(newMeta.getName());
    // rename field name
    if (Objects.nonNull(lookupValue) && !Utils.isEmpty(lookupValue.getRename())) {
      newMeta.setName(lookupValue.getRename());
    }

    // add or replace valueMeta
    if (index == -1) {
      inputRowMeta.addValueMeta(newMeta);
    } else {
      inputRowMeta.setValueMeta(index, newMeta);
    }
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;

    if (prev != null && !prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "FuzzyMatchMeta.CheckResult.TransformReceivingFields", prev.size() + ""),
              transformMeta);
      remarks.add(cr);

      // Starting from selected fields in ...
      // Check the fields from the previous stream!
      String mainField = variables.resolve(getMainStreamField());
      int idx = prev.indexOfValue(mainField);
      if (idx < 0) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "FuzzyMatchMeta.CheckResult.MainFieldNotFound", mainField),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "FuzzyMatchMeta.CheckResult.MainFieldFound", mainField),
                transformMeta);
      }
      remarks.add(cr);

    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "FuzzyMatchMeta.CheckResult.CouldNotFindFieldsFromPreviousTransforms"),
              transformMeta);
      remarks.add(cr);
    }

    if (info != null && !info.isEmpty()) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "FuzzyMatchMeta.CheckResult.TransformReceivingLookupData", info.size() + ""),
              transformMeta));

      // Check the fields from the lookup stream!
      String realLookupField = variables.resolve(getLookupField());

      int idx = info.indexOfValue(realLookupField);
      if (idx < 0) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "FuzzyMatchMeta.CheckResult.FieldNotFoundInLookupStream", realLookupField),
                transformMeta));
      } else {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "FuzzyMatchMeta.CheckResult.FieldFoundInTheLookupStream", realLookupField),
                transformMeta));
      }

      StringBuilder errorMessage = new StringBuilder();
      boolean errorFound = false;

      // Check the values to retrieve from the lookup stream!
      for (FMLookupValue lookupValue : lookupValues) {
        idx = info.indexOfValue(lookupValue.getName());
        if (idx < 0) {
          errorMessage.append("\t\t").append(lookupValue.getName()).append(Const.CR);
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage.insert(
            0,
            BaseMessages.getString(PKG, "FuzzyMatchMeta.CheckResult.FieldsNotFoundInLookupStream2")
                + Const.CR
                + Const.CR);

        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR, errorMessage.toString(), transformMeta));
      } else {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "FuzzyMatchMeta.CheckResult.AllFieldsFoundInTheLookupStream2"),
                transformMeta));
      }
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "FuzzyMatchMeta.CheckResult.FieldsNotFoundFromInLookupSep"),
              transformMeta));
    }

    // See if the source transform is filled in!
    IStream infoStream = getTransformIOMeta().getInfoStreams().get(0);
    if (infoStream.getTransformMeta() == null) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "FuzzyMatchMeta.CheckResult.SourceTransformNotSelected"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "FuzzyMatchMeta.CheckResult.SourceTransformIsSelected"),
              transformMeta));

      // See if the transform exists!
      //
      if (info != null) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "FuzzyMatchMeta.CheckResult.SourceTransformExist",
                    infoStream.getTransformName() + ""),
                transformMeta));
      } else {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "FuzzyMatchMeta.CheckResult.SourceTransformDoesNotExist",
                    infoStream.getTransformName() + ""),
                transformMeta));
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length >= 2) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "FuzzyMatchMeta.CheckResult.TransformReceivingInfoFromInputTransforms",
                  input.length + ""),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "FuzzyMatchMeta.CheckResult.NeedAtLeast2InputStreams", Const.CR, Const.CR),
              transformMeta));
    }
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    for (IStream stream : infoStreams) {
      stream.setTransformMeta(TransformMeta.findTransform(transforms, stream.getSubject()));
    }
  }

  @Override
  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * Returns the Input/Output metadata for this transform. The generator transform only produces
   * output, does not accept input!
   */
  @Override
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta(false);
    if (ioMeta == null) {

      ioMeta = new TransformIOMeta(true, true, false, false, false, false);

      IStream stream =
          new Stream(
              StreamType.INFO,
              null,
              BaseMessages.getString(PKG, "FuzzyMatchMeta.InfoStream.Description"),
              StreamIcon.INFO,
              lookupTransformName);
      ioMeta.addStream(stream);
      setTransformIOMeta(ioMeta);
    }

    return ioMeta;
  }

  @Getter
  public enum MatchMode implements IEnumHasCodeAndDescription {
    CLOSEST("closest", BaseMessages.getString(PKG, "FuzzyMatchMeta.matchMode.Closest")),
    ALL_ROWS("all_rows", BaseMessages.getString(PKG, "FuzzyMatchMeta.matchMode.AllRows")),
    ALL_CONCAT("all_concat", BaseMessages.getString(PKG, "FuzzyMatchMeta.matchMode.AllConcat")),
    ;
    private final String code;
    private final String description;

    MatchMode(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return Arrays.stream(MatchMode.values())
          .map(MatchMode::getDescription)
          .toArray(String[]::new);
    }

    public static MatchMode lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(MatchMode.class, description, CLOSEST);
    }

    public static MatchMode lookupCode(String code) {
      return IEnumHasCode.lookupCode(MatchMode.class, code, CLOSEST);
    }
  }

  @Getter
  public enum Algorithm implements IEnumHasCodeAndDescription {
    NONE("", ""),
    LEVENSHTEIN("levenshtein", BaseMessages.getString(PKG, "FuzzyMatchMeta.algorithm.Levenshtein")),
    DAMERAU_LEVENSHTEIN(
        "dameraulevenshtein",
        BaseMessages.getString(PKG, "FuzzyMatchMeta.algorithm.DamerauLevenshtein")),
    NEEDLEMAN_WUNSH(
        "needlemanwunsch", BaseMessages.getString(PKG, "FuzzyMatchMeta.algorithm.NeedlemanWunsch")),
    JARO("jaro", BaseMessages.getString(PKG, "FuzzyMatchMeta.algorithm.Jaro")),
    JARO_WINKLER(
        "jarowinkler", BaseMessages.getString(PKG, "FuzzyMatchMeta.algorithm.JaroWinkler")),
    PAIR_SIMILARITY(
        "pairsimilarity", BaseMessages.getString(PKG, "FuzzyMatchMeta.algorithm.PairSimilarity")),
    METAPHONE("metaphone", BaseMessages.getString(PKG, "FuzzyMatchMeta.algorithm.Metaphone")),
    DOUBLE_METAPHONE(
        "doublemataphone", BaseMessages.getString(PKG, "FuzzyMatchMeta.algorithm.DoubleMetaphone")),
    SOUNDEX("soundex", BaseMessages.getString(PKG, "FuzzyMatchMeta.algorithm.SoundEx")),
    REFINED_SOUNDEX(
        "refinedsoundex", BaseMessages.getString(PKG, "FuzzyMatchMeta.algorithm.RefinedSoundEx")),
    ;
    private final String code;
    private final String description;

    Algorithm(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return Arrays.stream(Algorithm.values())
          .filter(t -> t != Algorithm.NONE)
          .map(Algorithm::getDescription)
          .toArray(String[]::new);
    }

    public static Algorithm lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(Algorithm.class, description, NONE);
    }

    public static Algorithm lookupCode(String code) {
      return IEnumHasCode.lookupCode(Algorithm.class, code, NONE);
    }
  }

  @Getter
  @Setter
  public static final class FMLookupValue {
    @HopMetadataProperty(key = "name")
    private String name;

    @HopMetadataProperty(key = "rename")
    private String rename;

    public FMLookupValue() {}

    public FMLookupValue(FMLookupValue v) {
      this.name = v.name;
      this.rename = v.rename;
    }

    public FMLookupValue(String name, String rename) {
      this.name = name;
      this.rename = rename;
    }
  }
}
