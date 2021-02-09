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

package org.apache.hop.pipeline.transforms.coalesce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

/**
 * Lets you combine multiple fields into one, selecting the first value that is non-null.
 */
@Transform(
    id = "Coalesce",
    name = "i18n::Coalesce.Name",
    description = "i18n::Coalesce.Description",
    image = "coalesce.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/coalesce.html")
@InjectionSupported(
    localizationPrefix = "CoalesceMeta.Injection.",
    groups = {"FIELDS"})
public class CoalesceMeta extends BaseTransformMeta
    implements ITransformMeta<CoalesceTransform, CoalesceData> {

  private static final Class<?> PKG = CoalesceMeta.class; // for i18n purposes

  private static final String TAG_NAME = "name";
  private static final String TAG_FIELD = "field";
  private static final String TAG_FIELDS = "fields";
  private static final String TAG_INPUT = "input";
  private static final String TAG_VALUE_TYPE = "value_type";
  private static final String TAG_EMPTY_IS_NULL = "empty_is_null";
  private static final String TAG_REMOVE = "remove";

  /** The fields to coalesce */
  @InjectionDeep private List<Coalesce> coalesces = new ArrayList<>();

  /** additional options */
  @Injection(name = "EMPTY_STRING_AS_NULLS")
  private boolean emptyStringsAsNulls;

  public CoalesceMeta() {
    super();
    
  }

  public CoalesceMeta(CoalesceMeta cloned) {
    super();
    this.emptyStringsAsNulls = cloned.emptyStringsAsNulls;    
    Iterator<Coalesce> iterator = cloned.coalesces.iterator();   
    while(iterator.hasNext())
    {
      coalesces.add(new Coalesce(iterator.next()));  
    }
  }
  
  @Override
  public CoalesceTransform createTransform(
      TransformMeta transformMeta,
      CoalesceData data,
      int  copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new CoalesceTransform(transformMeta, this, data,  copyNr, pipelineMeta, pipeline);
  }

  @Override
  public CoalesceData getTransformData() {
    return new CoalesceData();
  }

  @Override
  public void setDefault() {
    this.coalesces = new ArrayList<>();
    this.emptyStringsAsNulls = false;
  }

  public boolean isTreatEmptyStringsAsNulls() {
    return this.emptyStringsAsNulls;
  }

  public void setTreatEmptyStringsAsNulls(boolean value) {
    this.emptyStringsAsNulls = value;
  }

  @Override
  public Object clone() {
    return new CoalesceMeta(this);
  }

  @Override
  public String getXml() throws HopValueException {

    StringBuilder xml = new StringBuilder(500);

    xml.append(XmlHandler.addTagValue(TAG_EMPTY_IS_NULL, emptyStringsAsNulls));

    xml.append(XmlHandler.openTag(TAG_FIELDS));
    for (Coalesce coalesce : coalesces) {
      xml.append(XmlHandler.openTag(TAG_FIELD));

      xml.append(XmlHandler.addTagValue(TAG_NAME, coalesce.getName()));
      xml.append(XmlHandler.addTagValue(TAG_VALUE_TYPE, ValueMetaFactory.getValueMetaName(coalesce.getType())));
      xml.append(XmlHandler.addTagValue(TAG_REMOVE, coalesce.isRemoveFields()));

      xml.append(XmlHandler.openTag(TAG_INPUT));
      for (String field : coalesce.getInputFields()) {
        xml.append(XmlHandler.addTagValue(TAG_FIELD, field));
      }
      xml.append(XmlHandler.closeTag(TAG_INPUT));

      xml.append(XmlHandler.closeTag(TAG_FIELD));
    }
    xml.append(XmlHandler.closeTag(TAG_FIELDS));

    return xml.toString();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {

    try {
      this.emptyStringsAsNulls = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, TAG_EMPTY_IS_NULL));

      Node fields = XmlHandler.getSubNode(transformNode, TAG_FIELDS);
      int count = XmlHandler.countNodes(fields, TAG_FIELD);
      coalesces = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        Node line = XmlHandler.getSubNodeByNr(fields, TAG_FIELD, i);

        Coalesce coalesce = new Coalesce();
        coalesce.setName(Const.NVL(XmlHandler.getTagValue(line, TAG_NAME), ""));
        coalesce.setType(XmlHandler.getTagValue(line, TAG_VALUE_TYPE));
        coalesce.setRemoveFields("Y".equalsIgnoreCase(XmlHandler.getTagValue(line, TAG_REMOVE)));

        Node input = XmlHandler.getSubNode(line, TAG_INPUT);
        if (input != null) {
          Node field = input.getFirstChild();
          while (field != null) {
            coalesce.addInputField(XmlHandler.getNodeValue(field));
            field = field.getNextSibling();
          }
        }

        coalesces.add(coalesce);
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "CoalesceMeta.Exception.UnableToReadXML"), e);
    }
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
      for (Coalesce coalesce : this.getCoalesces()) {

        if (coalesce.isRemoveFields()) {

          // Resolve variable name
          String name = variables.resolve(coalesce.getName());

          for (String fieldName : coalesce.getInputFields()) {

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
      for (Coalesce coalesce : this.getCoalesces()) {
        int type = coalesce.getType();
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
                  PKG, "CoalesceMeta.CheckResult.ReceivingFieldsFromPreviousTransforms", prev.size()),
              transformMeta));
    }

    // See if there are input streams leading to this step!
    if (input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "CoalesceMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
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
    for (Coalesce coalesce : this.getCoalesces()) {

      Set<String> fields = new HashSet<>();
      List<String> missingFields = new ArrayList<>();
      List<String> duplicateFields = new ArrayList<>();

      for (String fieldName : coalesce.getInputFields()) {

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
    if (this.getCoalesces().isEmpty()) {
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
  private int findDefaultValueType(final IRowMeta inputRowMeta, final Coalesce coalesce) {

    int type = IValueMeta.TYPE_NONE;
    boolean first = true;

    for (String field : coalesce.getInputFields()) {

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

  public List<Coalesce> getCoalesces() {
    return coalesces;
  }

  public void setCoalesces(List<Coalesce> coalesces) {
    this.coalesces = (coalesces == null) ? Collections.emptyList() : coalesces;
  }
}
