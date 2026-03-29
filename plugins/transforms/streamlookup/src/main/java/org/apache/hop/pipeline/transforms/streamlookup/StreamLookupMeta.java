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

package org.apache.hop.pipeline.transforms.streamlookup;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
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
    id = "StreamLookup",
    image = "streamlookup.svg",
    name = "i18n::StreamLookup.Name",
    description = "i18n::StreamLookup.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    keywords = "i18n::StreamLookupMeta.keyword",
    documentationUrl = "/pipeline/transforms/streamlookup.html")
@Getter
@Setter
public class StreamLookupMeta extends BaseTransformMeta<StreamLookup, StreamLookupData> {
  private static final Class<?> PKG = StreamLookupMeta.class;
  public static final String CONST_SPACES = "        ";

  @HopMetadataProperty(
      key = "from",
      injectionKey = "SOURCE_TRANSFORM",
      injectionKeyDescription = "StreamLookupMeta.Injection.SOURCE_TRANSFORM")
  private String sourceTransformName;

  /** Indicate that the input is considered sorted! */
  @HopMetadataProperty(
      key = "input_sorted",
      injectionKey = "INPUT_SORTED",
      injectionKeyDescription = "StreamLookupMeta.Injection.INPUT_SORTED")
  private boolean inputSorted;

  /** Indicate that we need to preserve memory by serializing objects */
  @HopMetadataProperty(
      key = "preserve_memory",
      injectionKey = "PRESERVE_MEMORY",
      injectionKeyDescription = "StreamLookupMeta.Injection.PRESERVE_MEMORY")
  private boolean memoryPreservationActive;

  /** Indicate that we want to use a sorted list vs. a hashtable */
  @HopMetadataProperty(
      key = "sorted_list",
      injectionKey = "SORTED_LIST",
      injectionKeyDescription = "StreamLookupMeta.Injection.SORTED_LIST")
  private boolean usingSortedList;

  /** The content of the key and lookup is a single Integer (long) */
  @HopMetadataProperty(
      key = "integer_pair",
      injectionKey = "INTEGER_PAIR",
      injectionKeyDescription = "StreamLookupMeta.Injection.INTEGER_PAIR")
  private boolean usingIntegerPair;

  @HopMetadataProperty(key = "lookup")
  private Lookup lookup;

  public StreamLookupMeta() {
    super();
    memoryPreservationActive = true;
    lookup = new Lookup();
  }

  public StreamLookupMeta(StreamLookupMeta m) {
    this();
    this.sourceTransformName = m.sourceTransformName;
    this.inputSorted = m.inputSorted;
    this.memoryPreservationActive = m.memoryPreservationActive;
    this.usingSortedList = m.usingSortedList;
    this.usingIntegerPair = m.usingIntegerPair;
    this.lookup = new Lookup(m.lookup);
  }

  @Override
  public Object clone() {
    return new StreamLookupMeta(this);
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    for (IStream stream : infoStreams) {
      stream.setTransformMeta(TransformMeta.findTransform(transforms, stream.getSubject()));
    }
  }

  @Override
  public void getFields(
      IRowMeta row,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    if (info != null && info.length == 1 && info[0] != null) {
      for (ReturnValue value : lookup.returnValues) {
        IValueMeta valueMeta = info[0].searchValueMeta(value.getValue());
        if (valueMeta != null) {
          // Configuration error/missing resources...
          valueMeta.setName(value.getValueName());
          valueMeta.setOrigin(origin);
          row.addValueMeta(valueMeta);
        } else {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, "StreamLookupMeta.Exception.ReturnValueCanNotBeFound", value.getValue()));
        }
      }
    } else {
      for (ReturnValue value : lookup.returnValues) {
        try {
          IValueMeta v =
              ValueMetaFactory.createValueMeta(value.getValueName(), value.getValueDefaultType());
          v.setOrigin(origin);
          row.addValueMeta(v);
        } catch (Exception e) {
          throw new HopTransformException(e);
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
                  PKG, "StreamLookupMeta.CheckResult.TransformReceivingFields", prev.size() + ""),
              transformMeta);
      remarks.add(cr);

      String errorMessage = "";
      boolean errorFound = false;

      // Starting from selected fields in ...
      // Check the fields from the previous stream!
      for (MatchKey key : lookup.matchKeys) {
        int idx = prev.indexOfValue(key.keyStream);
        if (idx < 0) {
          errorMessage += "\t\t" + key.keyStream + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(PKG, "StreamLookupMeta.CheckResult.FieldsNotFound")
                + Const.CR
                + Const.CR
                + errorMessage;

        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "StreamLookupMeta.CheckResult.AllFieldsFound"),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "StreamLookupMeta.CheckResult.CouldNotFindFieldsFromPreviousTransforms"),
              transformMeta);
      remarks.add(cr);
    }

    if (info != null && !info.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "StreamLookupMeta.CheckResult.TransformReceivingLookupData",
                  info.size() + ""),
              transformMeta);
      remarks.add(cr);

      String errorMessage = "";
      boolean errorFound = false;

      // Check the fields from the lookup stream!
      for (MatchKey key : lookup.matchKeys) {
        int idx = info.indexOfValue(key.keyLookup);
        if (idx < 0) {
          errorMessage += "\t\t" + key.keyLookup + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(PKG, "StreamLookupMeta.CheckResult.FieldsNotFoundInLookupStream")
                + Const.CR
                + Const.CR
                + errorMessage;

        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "StreamLookupMeta.CheckResult.AllFieldsFoundInTheLookupStream"),
                transformMeta);
        remarks.add(cr);
      }

      // Check the values to retrieve from the lookup stream!
      for (ReturnValue value : lookup.returnValues) {
        int idx = info.indexOfValue(value.getValue());
        if (idx < 0) {
          errorMessage += "\t\t" + value.getValue() + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(
                    PKG, "StreamLookupMeta.CheckResult.FieldsNotFoundInLookupStream2")
                + Const.CR
                + Const.CR
                + errorMessage;

        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "StreamLookupMeta.CheckResult.AllFieldsFoundInTheLookupStream2"),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "StreamLookupMeta.CheckResult.FieldsNotFoundFromInLookupSep"),
              transformMeta);
      remarks.add(cr);
    }

    // See if the source transform is filled in!
    IStream infoStream = getTransformIOMeta().getInfoStreams().getFirst();
    if (infoStream.getTransformMeta() == null) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "StreamLookupMeta.CheckResult.SourceTransformNotSelected"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "StreamLookupMeta.CheckResult.SourceTransformIsSelected"),
              transformMeta);
      remarks.add(cr);

      // See if the transform exists!
      //
      if (info != null) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "StreamLookupMeta.CheckResult.SourceTransformExist",
                    infoStream.getTransformName()),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "StreamLookupMeta.CheckResult.SourceTransformDoesNotExist",
                    infoStream.getTransformName()),
                transformMeta);
        remarks.add(cr);
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length >= 2) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "StreamLookupMeta.CheckResult.TransformReceivingInfoFromInputTransforms",
                  input.length + ""),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "StreamLookupMeta.CheckResult.NeedAtLeast2InputStreams", Const.CR, Const.CR),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public boolean excludeFromRowLayoutVerification() {
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
              BaseMessages.getString(PKG, "StreamLookupMeta.InfoStream.Description"),
              StreamIcon.INFO,
              sourceTransformName);
      ioMeta.addStream(stream);
      setTransformIOMeta(ioMeta);
    }

    return ioMeta;
  }

  @Override
  public void resetTransformIoMeta() {
    // Do nothing, don't reset as there is no need to do this.
  }

  @Getter
  @Setter
  public static class Lookup {
    @HopMetadataProperty(
        key = "key",
        injectionGroupKey = "KEYS",
        injectionGroupDescription = "StreamLookupMeta.Injection.KEYS")
    private List<MatchKey> matchKeys;

    @HopMetadataProperty(
        key = "value",
        injectionGroupKey = "VALUES",
        injectionGroupDescription = "StreamLookupMeta.Injection.VALUES")
    private List<ReturnValue> returnValues;

    public Lookup() {
      matchKeys = new ArrayList<>();
      returnValues = new ArrayList<>();
    }

    public Lookup(Lookup l) {
      this();
      l.matchKeys.forEach(k -> matchKeys.add(new MatchKey(k)));
      l.returnValues.forEach(v -> returnValues.add(new ReturnValue(v)));
    }
  }

  @Getter
  @Setter
  public static class MatchKey {
    /** fields in input streams with which we look up values */
    @HopMetadataProperty(
        key = "name",
        injectionKey = "KEY_STREAM",
        injectionKeyDescription = "StreamLookupMeta.Injection.KEY_STREAM")
    private String keyStream;

    /** fields in lookup stream with which we look up values */
    @HopMetadataProperty(
        key = "field",
        injectionKey = "KEY_LOOKUP",
        injectionKeyDescription = "StreamLookupMeta.Injection.KEY_LOOKUP")
    private String keyLookup;

    public MatchKey() {}

    public MatchKey(MatchKey k) {
      this();
      this.keyStream = k.keyStream;
      this.keyLookup = k.keyLookup;
    }
  }

  @Getter
  @Setter
  public static class ReturnValue {
    /** return these field values from lookup */
    @HopMetadataProperty(
        key = "name",
        injectionKey = "RETRIEVE_VALUE",
        injectionKeyDescription = "StreamLookupMeta.Injection.RETRIEVE_VALUE")
    private String value;

    /** rename to this after lookup */
    @HopMetadataProperty(
        key = "rename",
        injectionKey = "RETRIEVE_VALUE_NAME",
        injectionKeyDescription = "StreamLookupMeta.Injection.RETRIEVE_VALUE_NAME")
    private String valueName;

    /** default value in case not found... */
    @HopMetadataProperty(
        key = "default",
        injectionKey = "RETRIEVE_VALUE_DEFAULT",
        injectionKeyDescription = "StreamLookupMeta.Injection.RETRIEVE_VALUE_DEFAULT")
    private String valueDefault;

    /** type of default value */
    @HopMetadataProperty(
        key = "type",
        intCodeConverter = ValueMetaBase.ValueTypeCodeConverter.class,
        injectionKey = "RETRIEVE_DEFAULT_TYPE",
        injectionKeyDescription = "StreamLookupMeta.Injection.RETRIEVE_DEFAULT_TYPE")
    private int valueDefaultType;

    public ReturnValue() {}

    public ReturnValue(ReturnValue v) {
      this();
      this.value = v.value;
      this.valueName = v.valueName;
      this.valueDefault = v.valueDefault;
      this.valueDefaultType = v.valueDefaultType;
    }
  }
}
