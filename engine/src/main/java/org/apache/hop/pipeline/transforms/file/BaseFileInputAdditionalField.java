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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Additional fields settings. */
public class BaseFileInputAdditionalField implements Cloneable {

  /** Additional fields */
  @HopMetadataProperty(
      key = "shortFileFieldName",
      injectionKey = "FILE_SHORT_FILE_FIELDNAME",
      injectionKeyDescription = "TextFileInput.Injection.FILE_SHORT_FILE_FIELDNAME")
  public String shortFilenameField;

  @HopMetadataProperty(
      key = "extensionFieldName",
      injectionKey = "FILE_EXTENSION_FIELDNAME",
      injectionKeyDescription = "TextFileInput.Injection.FILE_EXTENSION_FIELDNAME")
  public String extensionField;

  @HopMetadataProperty(
      key = "pathFieldName",
      injectionKey = "FILE_PATH_FIELDNAME",
      injectionKeyDescription = "TextFileInput.Injection.FILE_PATH_FIELDNAME")
  public String pathField;

  @HopMetadataProperty(
      key = "sizeFieldName",
      injectionKey = "FILE_SIZE_FIELDNAME",
      injectionKeyDescription = "TextFileInput.Injection.FILE_SIZE_FIELDNAME")
  public String sizeField;

  @HopMetadataProperty(
      key = "hiddenFieldName",
      injectionKey = "FILE_HIDDEN_FIELDNAME",
      injectionKeyDescription = "TextFileInput.Injection.FILE_HIDDEN_FIELDNAME")
  public String hiddenField;

  @HopMetadataProperty(
      key = "lastModificationTimeFieldName",
      injectionKey = "FILE_LAST_MODIFICATION_FIELDNAME",
      injectionKeyDescription = "TextFileInput.Injection.FILE_LAST_MODIFICATION_FIELDNAME")
  public String lastModificationField;

  @HopMetadataProperty(
      key = "uriNameFieldName",
      injectionKey = "FILE_URI_FIELDNAME",
      injectionKeyDescription = "TextFileInput.Injection.FILE_URI_FIELDNAME")
  public String uriField;

  @HopMetadataProperty(
      key = "rootUriNameFieldName",
      injectionKey = "FILE_ROOT_URI_FIELDNAME",
      injectionKeyDescription = "TextFileInput.Injection.FILE_ROOT_URI_FIELDNAME")
  public String rootUriField;

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException ex) {
      throw new IllegalArgumentException("Clone not supported for " + this.getClass().getName());
    }
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

  public String getShortFilenameField() {
    return shortFilenameField;
  }

  public void setShortFilenameField(String shortFilenameField) {
    this.shortFilenameField = shortFilenameField;
  }

  public String getExtensionField() {
    return extensionField;
  }

  public void setExtensionField(String extensionField) {
    this.extensionField = extensionField;
  }

  public String getPathField() {
    return pathField;
  }

  public void setPathField(String pathField) {
    this.pathField = pathField;
  }

  public String getSizeField() {
    return sizeField;
  }

  public void setSizeField(String sizeField) {
    this.sizeField = sizeField;
  }

  public String getHiddenField() {
    return hiddenField;
  }

  public void setHiddenField(String hiddenField) {
    this.hiddenField = hiddenField;
  }

  public String getLastModificationField() {
    return lastModificationField;
  }

  public void setLastModificationField(String lastModificationField) {
    this.lastModificationField = lastModificationField;
  }

  public String getUriField() {
    return uriField;
  }

  public void setUriField(String uriField) {
    this.uriField = uriField;
  }

  public String getRootUriField() {
    return rootUriField;
  }

  public void setRootUriField(String rootUriField) {
    this.rootUriField = rootUriField;
  }
}
