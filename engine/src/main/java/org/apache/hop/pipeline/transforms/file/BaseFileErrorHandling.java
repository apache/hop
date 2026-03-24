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
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Block for error handling settings. */
@Getter
@Setter
public class BaseFileErrorHandling implements Cloneable {
  /** Ignore error : turn into warnings */
  @HopMetadataProperty(
      key = "error_ignored",
      injectionKey = "IGNORE_ERRORS",
      injectionKeyDescription = "TextFileInput.Injection.IGNORE_ERRORS")
  private boolean errorIgnored;

  /** File error field name. */
  @HopMetadataProperty(
      key = "file_error_field",
      injectionKey = "FILE_ERROR_FIELD",
      injectionKeyDescription = "TextFileInput.Injection.FILE_ERROR_FIELD")
  private String fileErrorField;

  /** File error text field name. */
  @HopMetadataProperty(
      key = "file_error_message_field",
      injectionKey = "FILE_ERROR_MESSAGE_FIELD",
      injectionKeyDescription = "TextFileInput.Injection.FILE_ERROR_MESSAGE_FIELD")
  private String fileErrorMessageField;

  @HopMetadataProperty(
      key = "skip_bad_files",
      injectionKey = "SKIP_BAD_FILES",
      injectionKeyDescription = "TextFileInput.Injection.SKIP_BAD_FILES")
  private boolean skipBadFiles;

  /** The directory that will contain warning files */
  @HopMetadataProperty(
      key = "bad_line_files_destination_directory",
      injectionKey = "WARNING_FILES_TARGET_DIR",
      injectionKeyDescription = "TextFileInput.Injection.WARNING_FILES_TARGET_DIR")
  private String warningFilesDestinationDirectory;

  /** The extension of warning files */
  @HopMetadataProperty(
      key = "bad_line_files_extension",
      injectionKey = "WARNING_FILES_EXTENTION",
      injectionKeyDescription = "TextFileInput.Injection.WARNING_FILES_EXTENTION")
  private String warningFilesExtension;

  /** The directory that will contain error files */
  @HopMetadataProperty(
      key = "error_line_files_destination_directory",
      injectionKey = "ERROR_FILES_TARGET_DIR",
      injectionKeyDescription = "TextFileInput.Injection.ERROR_FILES_TARGET_DIR")
  private String errorFilesDestinationDirectory;

  /** The extension of error files */
  @HopMetadataProperty(
      key = "error_line_files_extension",
      injectionKey = "ERROR_FILES_EXTENTION",
      injectionKeyDescription = "TextFileInput.Injection.ERROR_FILES_EXTENTION")
  private String errorFilesExtension;

  /** The directory that will contain line number files */
  @HopMetadataProperty(
      key = "line_number_files_destination_directory",
      injectionKey = "LINE_NR_FILES_TARGET_DIR",
      injectionKeyDescription = "TextFileInput.Injection.LINE_NR_FILES_TARGET_DIR")
  private String lineNumberFilesDestinationDirectory;

  /** The extension of line number files */
  @HopMetadataProperty(
      key = "line_number_files_extension",
      injectionKey = "LINE_NR_FILES_EXTENTION",
      injectionKeyDescription = "TextFileInput.Injection.LINE_NR_FILES_EXTENTION")
  private String lineNumberFilesExtension;

  public BaseFileErrorHandling() {}

  public BaseFileErrorHandling(BaseFileErrorHandling b) {
    this.errorFilesDestinationDirectory = b.errorFilesDestinationDirectory;
    this.errorFilesExtension = b.errorFilesExtension;
    this.errorIgnored = b.errorIgnored;
    this.fileErrorField = b.fileErrorField;
    this.fileErrorMessageField = b.fileErrorMessageField;
    this.lineNumberFilesDestinationDirectory = b.lineNumberFilesDestinationDirectory;
    this.lineNumberFilesExtension = b.lineNumberFilesExtension;
    this.skipBadFiles = b.skipBadFiles;
    this.warningFilesDestinationDirectory = b.warningFilesDestinationDirectory;
    this.warningFilesExtension = b.warningFilesExtension;
  }

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException ex) {
      throw new IllegalArgumentException("Clone not supported for " + this.getClass().getName());
    }
  }
}
