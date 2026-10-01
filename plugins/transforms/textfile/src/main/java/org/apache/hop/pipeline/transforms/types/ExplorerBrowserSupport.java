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

package org.apache.hop.pipeline.transforms.types;

import java.util.Locale;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.ui.hopgui.perspective.explorer.web.HopWebExplorerFileHelper;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.browser.Browser;

/** Loads a file into an explorer {@link Browser} with a real document URL when possible. */
final class ExplorerBrowserSupport {

  private ExplorerBrowserSupport() {}

  static boolean isHttpUrl(String filename) {
    if (filename == null) {
      return false;
    }
    String lower = filename.toLowerCase(Locale.ROOT);
    return lower.startsWith("http://") || lower.startsWith("https://");
  }

  /**
   * {@code Browser.setUrl} when the filename is http(s), a Hop Web explorer-file URL can be built,
   * or the VFS URL is fetchable by a desktop browser ({@code file:}/{@code http:}/{@code https:}).
   *
   * @return true when the browser was given a URL
   */
  static boolean loadInBrowser(Browser browser, String filename, IVariables variables)
      throws Exception {
    if (browser == null || filename == null) {
      return false;
    }
    if (isHttpUrl(filename)) {
      browser.setUrl(filename);
      return true;
    }
    if (EnvironmentUtils.getInstance().isWeb()) {
      String url = HopWebExplorerFileHelper.urlFor(filename, variables);
      if (url != null) {
        browser.setUrl(url);
        return true;
      }
      return false;
    }
    FileObject fileObject = HopVfs.getFileObject(filename, variables);
    String url = fileObject.getURL().toString();
    if (isBrowserFetchable(url)) {
      browser.setUrl(url);
      return true;
    }
    return false;
  }

  static boolean isBrowserFetchable(String url) {
    if (url == null) {
      return false;
    }
    String lower = url.toLowerCase(Locale.ROOT);
    return lower.startsWith("file:") || lower.startsWith("http:") || lower.startsWith("https:");
  }
}
