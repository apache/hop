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

import org.apache.hop.metadata.api.HopMetadataProperty;

/** Block for error handling settings. */
public class BaseFileErrorHandling implements Cloneable {

  /** Ignore error : turn into warnings */
  @HopMetadataProperty(
      key = "error_ignored",
      injectionKey = "IGNORE_ERRORS",
      injectionKeyDescription = "TextFileInput.Injection.IGNORE_ERRORS")
  public boolean errorIgnored;

  /** File error field name. */
  @HopMetadataProperty(
      key = "error_file_name",
      injectionKey = "FILE_ERROR_FIELD",
      injectionKeyDescription = "TextFileInput.Injection.FILE_ERROR_FIELD")
  public String fileErrorField;

  /** File error text field name. */
  @HopMetadataProperty(
      key = "file_error_message_field",
      injectionKey = "FILE_ERROR_MESSAGE_FIELD",
      injectionKeyDescription = "TextFileInput.Injection.FILE_ERROR_MESSAGE_FIELD")
  public String fileErrorMessageField;

  @HopMetadataProperty(
      key = "skip_bad_files",
      injectionKey = "SKIP_BAD_FILES",
      injectionKeyDescription = "TextFileInput.Injection.SKIP_BAD_FILES")
  public boolean skipBadFiles;

  /** The directory that will contain warning files */
  @HopMetadataProperty(
      key = "bad_line_files_destination_directory",
      injectionKey = "WARNING_FILES_TARGET_DIR",
      injectionKeyDescription = "TextFileInput.Injection.WARNING_FILES_TARGET_DIR")
  public String warningFilesDestinationDirectory;

  /** The extension of warning files */
  @HopMetadataProperty(
      key = "bad_line_files_extension",
      injectionKey = "WARNING_FILES_EXTENTION",
      injectionKeyDescription = "TextFileInput.Injection.WARNING_FILES_EXTENTION")
  public String warningFilesExtension;

  /** The directory that will contain error files */
  @HopMetadataProperty(
      key = "error_line_files_destination_directory",
      injectionKey = "ERROR_FILES_TARGET_DIR",
      injectionKeyDescription = "TextFileInput.Injection.ERROR_FILES_TARGET_DIR")
  public String errorFilesDestinationDirectory;

  /** The extension of error files */
  @HopMetadataProperty(
      key = "error_line_files_extension",
      injectionKey = "ERROR_FILES_EXTENTION",
      injectionKeyDescription = "TextFileInput.Injection.ERROR_FILES_EXTENTION")
  public String errorFilesExtension;

  /** The directory that will contain line number files */
  @HopMetadataProperty(
      key = "line_number_files_destination_directory",
      injectionKey = "LINE_NR_FILES_TARGET_DIR",
      injectionKeyDescription = "TextFileInput.Injection.LINE_NR_FILES_TARGET_DIR")
  public String lineNumberFilesDestinationDirectory;

  /** The extension of line number files */
  @HopMetadataProperty(
      key = "line_number_files_extension",
      injectionKey = "LINE_NR_FILES_EXTENTION",
      injectionKeyDescription = "TextFileInput.Injection.LINE_NR_FILES_EXTENTION")
  public String lineNumberFilesExtension;

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException ex) {
      throw new IllegalArgumentException("Clone not supported for " + this.getClass().getName());
    }
  }

  public boolean isErrorIgnored() {
    return errorIgnored;
  }

  public void setErrorIgnored(boolean errorIgnored) {
    this.errorIgnored = errorIgnored;
  }

  public String getFileErrorField() {
    return fileErrorField;
  }

  public void setFileErrorField(String fileErrorField) {
    this.fileErrorField = fileErrorField;
  }

  public String getFileErrorMessageField() {
    return fileErrorMessageField;
  }

  public void setFileErrorMessageField(String fileErrorMessageField) {
    this.fileErrorMessageField = fileErrorMessageField;
  }

  public boolean isSkipBadFiles() {
    return skipBadFiles;
  }

  public void setSkipBadFiles(boolean skipBadFiles) {
    this.skipBadFiles = skipBadFiles;
  }

  public String getWarningFilesDestinationDirectory() {
    return warningFilesDestinationDirectory;
  }

  public void setWarningFilesDestinationDirectory(String warningFilesDestinationDirectory) {
    this.warningFilesDestinationDirectory = warningFilesDestinationDirectory;
  }

  public String getWarningFilesExtension() {
    return warningFilesExtension;
  }

  public void setWarningFilesExtension(String warningFilesExtension) {
    this.warningFilesExtension = warningFilesExtension;
  }

  public String getErrorFilesDestinationDirectory() {
    return errorFilesDestinationDirectory;
  }

  public void setErrorFilesDestinationDirectory(String errorFilesDestinationDirectory) {
    this.errorFilesDestinationDirectory = errorFilesDestinationDirectory;
  }

  public String getErrorFilesExtension() {
    return errorFilesExtension;
  }

  public void setErrorFilesExtension(String errorFilesExtension) {
    this.errorFilesExtension = errorFilesExtension;
  }

  public String getLineNumberFilesDestinationDirectory() {
    return lineNumberFilesDestinationDirectory;
  }

  public void setLineNumberFilesDestinationDirectory(String lineNumberFilesDestinationDirectory) {
    this.lineNumberFilesDestinationDirectory = lineNumberFilesDestinationDirectory;
  }

  public String getLineNumberFilesExtension() {
    return lineNumberFilesExtension;
  }

  public void setLineNumberFilesExtension(String lineNumberFilesExtension) {
    this.lineNumberFilesExtension = lineNumberFilesExtension;
  }
}
