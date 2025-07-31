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
import java.util.List;
import org.apache.commons.lang.StringUtils;
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

  /** get closer matching value */
  @HopMetadataProperty(key = "closervalue")
  private boolean closerValue;

  /** return these field values from lookup */
  @HopMetadataProperty(groupKey = "lookup", key = "value")
  private List<FMLookupValue> lookupValues;

  public FuzzyMatchMeta() {
    super();
    this.algorithm = NONE;
    this.lookupValues = new ArrayList<>();
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
    this.closerValue = m.closerValue;
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
    closerValue = true;
    minimalValue = "0";
    maximalValue = "1";
    caseSensitive = false;
    lookupField = null;
    mainStreamField = null;
    outputMatchField = BaseMessages.getString(PKG, "FuzzyMatchMeta.OutputMatchFieldname");
    outputValueField = BaseMessages.getString(PKG, "FuzzyMatchMeta.OutputValueFieldname");
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
    if (StringUtils.isNotEmpty(mainField) && isCloserValue()) {
      switch (getAlgorithm()) {
        case NONE:
          throw new HopTransformException("Please specify the matching algorithm to use");
        case LEVENSHTEIN:
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
      v.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }

    boolean activateAdditionalFields =
        isCloserValue()
            || (getAlgorithm() == DOUBLE_METAPHONE)
            || (getAlgorithm() == SOUNDEX)
            || (getAlgorithm() == REFINED_SOUNDEX)
            || (getAlgorithm() == METAPHONE);

    if (activateAdditionalFields) {
      if (info != null && info.length == 1 && info[0] != null) {
        for (FMLookupValue lookupValue : lookupValues) {
          v = info[0].searchValueMeta(lookupValue.getName());
          if (v != null) {
            // Configuration error/missing resources...
            v.setName(lookupValue.getName());
            v.setOrigin(name);
            v.setStorageType(
                IValueMeta.STORAGE_TYPE_NORMAL); // Only normal storage goes into the cache
            inputRowMeta.addValueMeta(v);
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
          inputRowMeta.addValueMeta(v);
        }
      }
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

  /**
   * Gets algorithm
   *
   * @return value of algorithm
   */
  public Algorithm getAlgorithm() {
    return algorithm;
  }

  /**
   * Sets algorithm
   *
   * @param algorithm value of algorithm
   */
  public void setAlgorithm(Algorithm algorithm) {
    this.algorithm = algorithm;
  }

  /**
   * Gets lookupTransformName
   *
   * @return value of lookupTransformName
   */
  public String getLookupTransformName() {
    return lookupTransformName;
  }

  /**
   * Sets lookupTransformName
   *
   * @param lookupTransformName value of lookupTransformName
   */
  public void setLookupTransformName(String lookupTransformName) {
    this.lookupTransformName = lookupTransformName;
  }

  /**
   * Gets lookupField
   *
   * @return value of lookupField
   */
  public String getLookupField() {
    return lookupField;
  }

  /**
   * Sets lookupField
   *
   * @param lookupField value of lookupField
   */
  public void setLookupField(String lookupField) {
    this.lookupField = lookupField;
  }

  /**
   * Gets mainStreamField
   *
   * @return value of mainStreamField
   */
  public String getMainStreamField() {
    return mainStreamField;
  }

  /**
   * Sets mainStreamField
   *
   * @param mainStreamField value of mainStreamField
   */
  public void setMainStreamField(String mainStreamField) {
    this.mainStreamField = mainStreamField;
  }

  /**
   * Gets outputmatchfield
   *
   * @return value of outputmatchfield
   */
  public String getOutputMatchField() {
    return outputMatchField;
  }

  /**
   * Sets outputmatchfield
   *
   * @param outputMatchField value of outputmatchfield
   */
  public void setOutputMatchField(String outputMatchField) {
    this.outputMatchField = outputMatchField;
  }

  /**
   * Gets outputValueField
   *
   * @return value of outputValueField
   */
  public String getOutputValueField() {
    return outputValueField;
  }

  /**
   * Sets outputValueField
   *
   * @param outputValueField value of outputValueField
   */
  public void setOutputValueField(String outputValueField) {
    this.outputValueField = outputValueField;
  }

  /**
   * Gets caseSensitive
   *
   * @return value of caseSensitive
   */
  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  /**
   * Sets caseSensitive
   *
   * @param caseSensitive value of caseSensitive
   */
  public void setCaseSensitive(boolean caseSensitive) {
    this.caseSensitive = caseSensitive;
  }

  /**
   * Gets minimalValue
   *
   * @return value of minimalValue
   */
  public String getMinimalValue() {
    return minimalValue;
  }

  /**
   * Sets minimalValue
   *
   * @param minimalValue value of minimalValue
   */
  public void setMinimalValue(String minimalValue) {
    this.minimalValue = minimalValue;
  }

  /**
   * Gets maximalValue
   *
   * @return value of maximalValue
   */
  public String getMaximalValue() {
    return maximalValue;
  }

  /**
   * Sets maximalValue
   *
   * @param maximalValue value of maximalValue
   */
  public void setMaximalValue(String maximalValue) {
    this.maximalValue = maximalValue;
  }

  /**
   * Gets separator
   *
   * @return value of separator
   */
  public String getSeparator() {
    return separator;
  }

  /**
   * Sets separator
   *
   * @param separator value of separator
   */
  public void setSeparator(String separator) {
    this.separator = separator;
  }

  /**
   * Gets closerValue
   *
   * @return value of closerValue
   */
  public boolean isCloserValue() {
    return closerValue;
  }

  /**
   * Sets closerValue
   *
   * @param closerValue value of closerValue
   */
  public void setCloserValue(boolean closerValue) {
    this.closerValue = closerValue;
  }

  /**
   * Gets lookupValues
   *
   * @return value of lookupValues
   */
  public List<FMLookupValue> getLookupValues() {
    return lookupValues;
  }

  /**
   * Sets lookupValues
   *
   * @param lookupValues value of lookupValues
   */
  public void setLookupValues(List<FMLookupValue> lookupValues) {
    this.lookupValues = lookupValues;
  }

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
      return IEnumHasCodeAndDescription.getDescriptions(Algorithm.class);
    }

    public static Algorithm lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(Algorithm.class, description, NONE);
    }

    public static Algorithm lookupCode(String code) {
      return IEnumHasCode.lookupCode(Algorithm.class, code, NONE);
    }

    /**
     * Gets code
     *
     * @return value of code
     */
    @Override
    public String getCode() {
      return code;
    }

    /**
     * Gets description
     *
     * @return value of description
     */
    @Override
    public String getDescription() {
      return description;
    }
  }

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

    /**
     * Gets name
     *
     * @return value of name
     */
    public String getName() {
      return name;
    }

    /**
     * Sets name
     *
     * @param name value of name
     */
    public void setName(String name) {
      this.name = name;
    }

    /**
     * Gets rename
     *
     * @return value of rename
     */
    public String getRename() {
      return rename;
    }

    /**
     * Sets rename
     *
     * @param rename value of rename
     */
    public void setRename(String rename) {
      this.rename = rename;
    }
  }
}
