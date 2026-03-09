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
package org.apache.hop.ui.hopgui;

/**
 * Optional hook for Hop Web to update the browser URL when the active tab changes and to build
 * shareable URLs for the "Copy URL for this tab" action. When running in Hop Web, the RAP module
 * sets an implementation so that the URL reflects the current project and file.
 */
public interface IHopWebUrlUpdater {

  /**
   * Update the browser URL to reflect the given project and file (e.g. replaceState with
   * ?project=...&amp;file=...). Called when the user switches to a different tab.
   *
   * @param projectName current project name (namespace), or null if none
   * @param filePath path of the active file, or null if no file
   */
  void updateUrl(String projectName, String filePath);

  /**
   * Build a full URL string for the given project and file, for copying to clipboard.
   *
   * @param projectName project name, or null
   * @param filePath file path, or null
   * @return URL string, or null if not available
   */
  String buildUrl(String projectName, String filePath);

  /**
   * Copy text to the client's clipboard. In Hop Web this uses the browser clipboard API so the copy
   * actually works; no-op or fallback if not supported.
   *
   * @param text plain text to copy, must not be null
   */
  default void copyToClipboard(String text) {
    // Default: no-op (desktop uses GuiResource.toClipboard instead)
  }
}
