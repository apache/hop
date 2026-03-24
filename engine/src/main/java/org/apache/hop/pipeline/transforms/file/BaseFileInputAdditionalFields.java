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

package org.apache.hop.pipeline.transforms.file;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Additional fields settings. */
@Getter
@Setter
public class BaseFileInputAdditionalFields implements Cloneable {
  /** Additional fields */
  @HopMetadataProperty(
      key = "shortFileFieldName",
      injectionKey = "FILE_SHORT_FILE_FIELDNAME",
      injectionKeyDescription = "TextFileInput.Injection.FILE_SHORT_FILE_FIELDNAME")
  protected String shortFilenameField;

  @HopMetadataProperty(
      key = "extensionFieldName",
      injectionKey = "FILE_EXTENSION_FIELDNAME",
      injectionKeyDescription = "TextFileInput.Injection.FILE_EXTENSION_FIELDNAME")
  protected String extensionField;

  @HopMetadataProperty(
      key = "pathFieldName",
      injectionKey = "FILE_PATH_FIELDNAME",
      injectionKeyDescription = "TextFileInput.Injection.FILE_PATH_FIELDNAME")
  protected String pathField;

  @HopMetadataProperty(
      key = "sizeFieldName",
      injectionKey = "FILE_SIZE_FIELDNAME",
      injectionKeyDescription = "TextFileInput.Injection.FILE_SIZE_FIELDNAME")
  protected String sizeField;

  @HopMetadataProperty(
      key = "hiddenFieldName",
      injectionKey = "FILE_HIDDEN_FIELDNAME",
      injectionKeyDescription = "TextFileInput.Injection.FILE_HIDDEN_FIELDNAME")
  protected String hiddenField;

  @HopMetadataProperty(
      key = "lastModificationTimeFieldName",
      injectionKey = "FILE_LAST_MODIFICATION_FIELDNAME",
      injectionKeyDescription = "TextFileInput.Injection.FILE_LAST_MODIFICATION_FIELDNAME")
  protected String lastModificationField;

  @HopMetadataProperty(
      key = "uriNameFieldName",
      injectionKey = "FILE_URI_FIELDNAME",
      injectionKeyDescription = "TextFileInput.Injection.FILE_URI_FIELDNAME")
  protected String uriField;

  @HopMetadataProperty(
      key = "rootUriNameFieldName",
      injectionKey = "FILE_ROOT_URI_FIELDNAME",
      injectionKeyDescription = "TextFileInput.Injection.FILE_ROOT_URI_FIELDNAME")
  protected String rootUriField;

  public BaseFileInputAdditionalFields() {}

  public BaseFileInputAdditionalFields(BaseFileInputAdditionalFields f) {
    this();
    this.extensionField = f.extensionField;
    this.hiddenField = f.hiddenField;
    this.lastModificationField = f.lastModificationField;
    this.pathField = f.pathField;
    this.rootUriField = f.rootUriField;
    this.shortFilenameField = f.shortFilenameField;
    this.sizeField = f.sizeField;
    this.uriField = f.uriField;
  }

  @Override
  public Object clone() {
    return new BaseFileInputAdditionalFields(this);
  }

  /**
   * Set null for all empty field values to be able to fast check during transform processing. Need
   * to be executed once before processing.
   */
  public void normalize() {
    if (StringUtils.isBlank(shortFilenameField)) {
      shortFilenameField = null;
    }
    if (StringUtils.isBlank(extensionField)) {
      extensionField = null;
    }
    if (StringUtils.isBlank(pathField)) {
      pathField = null;
    }
    if (StringUtils.isBlank(sizeField)) {
      sizeField = null;
    }
    if (StringUtils.isBlank(hiddenField)) {
      hiddenField = null;
    }
    if (StringUtils.isBlank(lastModificationField)) {
      lastModificationField = null;
    }
    if (StringUtils.isBlank(uriField)) {
      uriField = null;
    }
    if (StringUtils.isBlank(rootUriField)) {
      rootUriField = null;
    }
  }

  public void getFields(IRowMeta r, String name, IVariables variables) {
    // TextFileInput is the same, this can be refactored further
    if (StringUtils.isNotEmpty(shortFilenameField)) {
      IValueMeta v = new ValueMetaString(variables.resolve(shortFilenameField));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(extensionField)) {
      IValueMeta v = new ValueMetaString(variables.resolve(extensionField));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(pathField)) {
      IValueMeta v = new ValueMetaString(variables.resolve(pathField));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(sizeField)) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(sizeField));
      v.setOrigin(name);
      v.setLength(9);
      r.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(hiddenField)) {
      IValueMeta v = new ValueMetaBoolean(variables.resolve(hiddenField));
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(lastModificationField)) {
      IValueMeta v = new ValueMetaDate(variables.resolve(lastModificationField));
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(uriField)) {
      IValueMeta v = new ValueMetaString(variables.resolve(uriField));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(rootUriField)) {
      IValueMeta v = new ValueMetaString(variables.resolve(rootUriField));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
  }
}
