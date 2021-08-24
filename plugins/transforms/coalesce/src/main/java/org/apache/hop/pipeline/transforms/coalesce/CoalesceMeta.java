/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.coalesce;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.*;

/** Lets you combine multiple fields into one, selecting the first value that is non-null. */
@Transform(
    id = "Coalesce",
    name = "i18n::Coalesce.Name",
    description = "i18n::Coalesce.Description",
    image = "coalesce.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "https://hop.apache.org/manual/latest/pipeline/transforms/coalesce.html")
public class CoalesceMeta extends BaseTransformMeta
    implements ITransformMeta<CoalesceTransform, CoalesceData> {

  private static final Class<?> PKG = CoalesceMeta.class; // for i18n purposes

  /** The fields to coalesce */
  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionGroupDescription = "CoalesceMeta.Injection.Fields",
      injectionKeyDescription = "CoalesceMeta.Injection.Field")
  private List<CoalesceField> fields = new ArrayList<>();

  /** additional options */
  @HopMetadataProperty(
      key = "empty_is_null",
      injectionKey = "EMPTY_STRING_AS_NULLS",
      injectionKeyDescription = "CoalesceMeta.Injection.EmptyStringAsNulls")
  private boolean treatEmptyStringsAsNulls;

  public CoalesceMeta() {
    fields = new ArrayList<>();
  }

  public CoalesceMeta(CoalesceMeta c) {
    super();
    this.treatEmptyStringsAsNulls = c.treatEmptyStringsAsNulls;
    for (CoalesceField field : c.getFields()) {
      fields.add(new CoalesceField(field));
    }
  }

  @Override
  public CoalesceTransform createTransform(
      TransformMeta transformMeta,
      CoalesceData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new CoalesceTransform(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public CoalesceData getTransformData() {
    return new CoalesceData();
  }

  @Override
  public void setDefault() {
    this.fields = new ArrayList<>();
    this.treatEmptyStringsAsNulls = false;
  }

  public boolean isTreatEmptyStringsAsNulls() {
    return this.treatEmptyStringsAsNulls;
  }

  public void setTreatEmptyStringsAsNulls(boolean value) {
    this.treatEmptyStringsAsNulls = value;
  }

  @Override
  public Object clone() {
    return new CoalesceMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String transformName,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    try {
      // store the input stream meta
      IRowMeta unalteredInputRowMeta = rowMeta.clone();

      // first remove all unwanted input fields from the stream
      for (CoalesceField coalesce : this.getFields()) {

        if (coalesce.isRemoveFields()) {

          // Resolve variable name
          String name = variables.resolve(coalesce.getName());

          for (String fieldName : coalesce.getInputFieldNames()) {

            // If input field name is recycled for output, don't
            // remove
            if (rowMeta.indexOfValue(name) != -1 && name.equals(fieldName)) continue;

            if (rowMeta.indexOfValue(fieldName) != -1) {
              rowMeta.removeValueMeta(fieldName);
            }
          }
        }
      }

      // then add the output fields
      for (CoalesceField coalesce : this.getFields()) {
        int type = ValueMetaFactory.getIdForValueMeta(coalesce.getType());
        if (type == IValueMeta.TYPE_NONE) {
          type = findDefaultValueType(unalteredInputRowMeta, coalesce);
        }

        String name = variables.resolve(coalesce.getName());
        IValueMeta valueMeta = ValueMetaFactory.createValueMeta(name, type);
        valueMeta.setOrigin(transformName);

        int index = rowMeta.indexOfValue(name);
        if (index >= 0) {
          rowMeta.setValueMeta(index, valueMeta);
        } else {
          rowMeta.addValueMeta(valueMeta);
        }
      }
    } catch (Exception e) {
      throw new HopTransformException(e);
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
    // See if we have fields from previous steps
    if (prev == null || prev.size() == 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(
                  PKG, "CoalesceMeta.CheckResult.NotReceivingFieldsFromPreviousTransforms"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "CoalesceMeta.CheckResult.ReceivingFieldsFromPreviousTransforms",
                  prev.size()),
              transformMeta));
    }

    // See if there are input streams leading to this transform!
    if (input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "CoalesceMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "CoalesceMeta.CheckResult.NotReceivingInfoFromOtherTransforms"),
              transformMeta));
    }

    // See if there are missing, duplicate or not enough input streams
    boolean missing = false;
    for (CoalesceField coalesce : this.getFields()) {

      Set<String> fields = new HashSet<>();
      List<String> missingFields = new ArrayList<>();
      List<String> duplicateFields = new ArrayList<>();

      for (String fieldName : coalesce.getInputFieldNames()) {

        if (fields.contains(fieldName)) duplicateFields.add(fieldName);
        else fields.add(fieldName);

        IValueMeta vmi = prev.searchValueMeta(fieldName);
        if (vmi == null) {
          missingFields.add(fieldName);
        }
      }

      if (!missingFields.isEmpty()) {
        String message =
            BaseMessages.getString(
                PKG,
                "CoalesceMeta.CheckResult.MissingInputFields",
                coalesce.getName(),
                StringUtils.join(missingFields, ','));
        remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, transformMeta));
        missing = true;
      } else if (!duplicateFields.isEmpty()) {
        String message =
            BaseMessages.getString(
                PKG,
                "CoalesceMeta.CheckResult.DuplicateInputFields",
                coalesce.getName(),
                StringUtils.join(duplicateFields, ','));
        remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, transformMeta));
        missing = true;
      } else if (fields.isEmpty()) {
        String message =
            BaseMessages.getString(
                PKG, "CoalesceMeta.CheckResult.EmptyInputFields", coalesce.getName());
        remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, transformMeta));
        missing = true;
      } else if (fields.size() < 2) {
        String message =
            BaseMessages.getString(
                PKG, "CoalesceMeta.CheckResult.NotEnoughInputFields", coalesce.getName());
        remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_WARNING, message, transformMeta));
      }
    }

    // See if there something to coalesce
    if (this.getFields().isEmpty()) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "CoalesceMeta.CheckResult.NothingToCoalesce"),
              transformMeta));
    } else if (!missing) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "CoalesceMeta.CheckResult.FoundAllInputFields"),
              transformMeta));
    }
  }

  /**
   * If all fields are of the same data type then the output field should mirror this otherwise
   * return a more generic String type
   */
  private int findDefaultValueType(final IRowMeta inputRowMeta, final CoalesceField coalesce) {

    int type = IValueMeta.TYPE_NONE;
    boolean first = true;

    for (String field : coalesce.getInputFieldNames()) {

      if (first) {
        type = getInputFieldValueType(inputRowMeta, field);
        first = false;
      } else {
        int otherType = getInputFieldValueType(inputRowMeta, field);

        if (type != otherType) {

          switch (type) {
            case IValueMeta.TYPE_STRING:
              // keep TYPE_STRING
              break;
            case IValueMeta.TYPE_INTEGER:
              if (otherType == IValueMeta.TYPE_NUMBER) {
                type = IValueMeta.TYPE_NUMBER;
              } else if (otherType == IValueMeta.TYPE_BIGNUMBER) {
                type = IValueMeta.TYPE_BIGNUMBER;
              } else {
                type = IValueMeta.TYPE_STRING;
              }
              break;
            case IValueMeta.TYPE_NUMBER:
              if (otherType == IValueMeta.TYPE_INTEGER) {
                // keep TYPE_NUMBER
              } else if (otherType == IValueMeta.TYPE_BIGNUMBER) {
                type = IValueMeta.TYPE_BIGNUMBER;
              } else {
                type = IValueMeta.TYPE_STRING;
              }
              break;

            case IValueMeta.TYPE_DATE:
              if (otherType == IValueMeta.TYPE_TIMESTAMP) {
                type = IValueMeta.TYPE_TIMESTAMP;
              } else {
                type = IValueMeta.TYPE_STRING;
              }
              break;
            case IValueMeta.TYPE_TIMESTAMP:
              if (otherType == IValueMeta.TYPE_DATE) {
                // keep TYPE_TIMESTAMP
              } else {
                type = IValueMeta.TYPE_STRING;
              }
              break;
            case IValueMeta.TYPE_BIGNUMBER:
              if (otherType == IValueMeta.TYPE_INTEGER) {
                // keep TYPE_BIGNUMBER
              } else if (otherType == IValueMeta.TYPE_NUMBER) {
                // keep TYPE_BIGNUMBER
              } else {
                type = IValueMeta.TYPE_STRING;
              }
              break;
            case IValueMeta.TYPE_BOOLEAN:
            case IValueMeta.TYPE_INET:
            case IValueMeta.TYPE_SERIALIZABLE:
            case IValueMeta.TYPE_BINARY:
            default:
              return IValueMeta.TYPE_STRING;
          }
        }
      }
    }

    if (type == IValueMeta.TYPE_NONE) {
      type = IValueMeta.TYPE_STRING;
    }

    return type;
  }

  /**
   * Extracts the ValueMeta type of an input field, returns {@link IValueMeta.TYPE_NONE} if the
   * field is not present in the input stream
   */
  private int getInputFieldValueType(final IRowMeta inputRowMeta, final String field) {
    int index = inputRowMeta.indexOfValue(field);
    if (index >= 0) {
      return inputRowMeta.getValueMeta(index).getType();
    }
    return IValueMeta.TYPE_NONE;
  }

  public List<CoalesceField> getFields() {
    return fields;
  }

  public void setFields(List<CoalesceField> fields) {
    this.fields = (fields == null) ? Collections.emptyList() : fields;
  }
}
