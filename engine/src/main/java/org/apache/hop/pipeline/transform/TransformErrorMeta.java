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

package org.apache.hop.pipeline.transform;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.changed.ChangedFlag;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.w3c.dom.Node;

/** This class contains the metadata to handle proper error handling on a transform level. */
@Getter
@Setter
public class TransformErrorMeta extends ChangedFlag implements Cloneable {
  public static final String XML_ERROR_TAG = "error";
  public static final String XML_SOURCE_TRANSFORM_TAG = "source_transform";
  public static final String XML_TARGET_TRANSFORM_TAG = "target_transform";

  /** The source transform that can send the error rows */
  @HopMetadataProperty(key = "source_transform", storeWithName = true, lookupInList = "transforms")
  private TransformMeta sourceTransform;

  /** The target transform to send the error rows to */
  @HopMetadataProperty(key = "target_transform", storeWithName = true, lookupInList = "transforms")
  private TransformMeta targetTransform;

  /** Is the error handling enabled? */
  @HopMetadataProperty(key = "is_enabled")
  private boolean enabled;

  /**
   * the name of the field value to contain the number of errors (null or empty means it's not
   * needed)
   */
  @HopMetadataProperty(key = "nr_valuename")
  private String nrErrorsValueName;

  /**
   * the name of the field value to contain the error description(s) (null or empty means it's not
   * needed)
   */
  @HopMetadataProperty(key = "descriptions_valuename")
  private String errorDescriptionsValueName;

  /**
   * the name of the field value to contain the fields for which the error(s) occured (null or empty
   * means it's not needed)
   */
  @HopMetadataProperty(key = "fields_valuename")
  private String errorFieldsValueName;

  /**
   * the name of the field value to contain the error code(s) (null or empty means it's not needed)
   */
  @HopMetadataProperty(key = "codes_valuename")
  private String errorCodesValueName;

  /** The maximum number of errors allowed before we stop processing with a hard error */
  @HopMetadataProperty(key = "max_errors")
  private String maxErrors = "";

  /** The maximum percent of errors allowed before we stop processing with a hard error */
  @HopMetadataProperty(key = "max_pct_errors")
  private String maxPercentErrors = "";

  /** The minimum number of rows to read before the percentage evaluation takes place */
  @HopMetadataProperty(key = "min_pct_rows")
  private String minPercentRows = "";

  public TransformErrorMeta() {}

  /**
   * Create a new transform error handling metadata object
   *
   * @param sourceTransform The source transform that can send the error rows
   */
  public TransformErrorMeta(TransformMeta sourceTransform) {
    this.sourceTransform = sourceTransform;
    this.enabled = false;
  }

  /**
   * Create a new transform error handling metadata object
   *
   * @param sourceTransform The source transform that can send the error rows
   * @param targetTransform The target transform to send the error rows to
   */
  public TransformErrorMeta(TransformMeta sourceTransform, TransformMeta targetTransform) {
    this.sourceTransform = sourceTransform;
    this.targetTransform = targetTransform;
    this.enabled = false;
  }

  /**
   * Create a new transform error handling metadata object
   *
   * @param sourceTransform The source transform that can send the error rows
   * @param targetTransform The target transform to send the error rows to
   * @param nrErrorsValueName the name of the field value to contain the number of errors (null or
   *     empty means it's not needed)
   * @param errorDescriptionsValueName the name of the field value to contain the error
   *     description(s) (null or empty means it's not needed)
   * @param errorFieldsValueName the name of the field value to contain the fields for which the
   *     error(s) occured (null or empty means it's not needed)
   * @param errorCodesValueName the name of the field value to contain the error code(s) (null or
   *     empty means it's not needed)
   */
  public TransformErrorMeta(
      TransformMeta sourceTransform,
      TransformMeta targetTransform,
      String nrErrorsValueName,
      String errorDescriptionsValueName,
      String errorFieldsValueName,
      String errorCodesValueName) {
    this.sourceTransform = sourceTransform;
    this.targetTransform = targetTransform;
    this.enabled = false;
    this.nrErrorsValueName = nrErrorsValueName;
    this.errorDescriptionsValueName = errorDescriptionsValueName;
    this.errorFieldsValueName = errorFieldsValueName;
    this.errorCodesValueName = errorCodesValueName;
  }

  @Override
  public TransformErrorMeta clone() {
    try {
      return (TransformErrorMeta) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new HopRuntimeException("Error cloning transform error metadata", e);
    }
  }

  public String getXml() throws HopException {
    return XmlHandler.aroundTag(XML_ERROR_TAG, XmlMetadataUtil.serializeObjectToXml(this));
  }

  @Getter
  public static class Transforms {
    private List<TransformMeta> transforms;
  }

  public TransformErrorMeta(Node node, List<TransformMeta> transforms) throws HopXmlException {
    // The deSerializeFromXml call searches for a List field called 'transforms' in the provided
    // Object.
    // Normally this is the transform metadata but here we wrap it a tiny class.
    //
    Transforms parentObject = new Transforms();
    parentObject.transforms = transforms;

    XmlMetadataUtil.deSerializeFromXml(
        parentObject, null, node, TransformErrorMeta.class, this, new MemoryMetadataProvider());
  }

  public IRowMeta getErrorRowMeta(IVariables variables) {
    IRowMeta row = new RowMeta();

    String nrErr = variables.resolve(getNrErrorsValueName());
    if (!Utils.isEmpty(nrErr)) {
      IValueMeta v = new ValueMetaInteger(nrErr);
      v.setLength(3);
      row.addValueMeta(v);
    }
    String errDesc = variables.resolve(getErrorDescriptionsValueName());
    if (!Utils.isEmpty(errDesc)) {
      IValueMeta v = new ValueMetaString(errDesc);
      row.addValueMeta(v);
    }
    String errFields = variables.resolve(getErrorFieldsValueName());
    if (!Utils.isEmpty(errFields)) {
      IValueMeta v = new ValueMetaString(errFields);
      row.addValueMeta(v);
    }
    String errCodes = variables.resolve(getErrorCodesValueName());
    if (!Utils.isEmpty(errCodes)) {
      IValueMeta v = new ValueMetaString(errCodes);
      row.addValueMeta(v);
    }
    return row;
  }

  public void addErrorRowData(
      IVariables variables,
      Object[] row,
      int startIndex,
      long nrErrors,
      String errorDescriptions,
      String fieldNames,
      String errorCodes) {
    int index = startIndex;

    String nrErr = variables.resolve(getNrErrorsValueName());
    if (!Utils.isEmpty(nrErr)) {
      row[index] = nrErrors;
      index++;
    }
    String errDesc = variables.resolve(getErrorDescriptionsValueName());
    if (!Utils.isEmpty(errDesc)) {
      row[index] = errorDescriptions;
      index++;
    }
    String errFields = variables.resolve(getErrorFieldsValueName());
    if (!Utils.isEmpty(errFields)) {
      row[index] = fieldNames;
      index++;
    }
    String errCodes = variables.resolve(getErrorCodesValueName());
    if (!Utils.isEmpty(errCodes)) {
      row[index] = errorCodes;
    }
  }
}
