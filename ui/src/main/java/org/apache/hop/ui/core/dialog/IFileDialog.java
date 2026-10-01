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

package org.apache.hop.ui.core.dialog;

public interface IFileDialog {
  void setText(String text);

  void setFilterExtensions(String[] filterExtensions);

  void setFilterNames(String[] filterNames);

  void setFileName(String fileName);

  String getFilterPath();

  String getFileName();

  String open();

  void setFilterPath(String filterPath);

  /**
   * Allow the user to select more than one file. This needs to be set before the dialog is opened.
   * Dialogs that can only ever select a single file simply ignore this.
   *
   * @param multiSelection true to allow selecting multiple files
   */
  default void setMultiSelection(boolean multiSelection) {
    // Single-selection dialogs have nothing to do here.
  }

  /**
   * The files that were selected, as full paths. This is only filled in when the dialog allows
   * multi-selection and the user actually selected more than one file. In every other case an empty
   * array is returned and {@link #getFileName()} (relative to {@link #getFilterPath()}) is the
   * selection.
   *
   * @return the full paths of the selected files, never null
   */
  default String[] getFileNames() {
    return new String[0];
  }
}
