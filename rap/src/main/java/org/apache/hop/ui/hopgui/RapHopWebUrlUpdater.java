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

import jakarta.servlet.http.HttpServletRequest;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.client.service.JavaScriptExecutor;

/**
 * RAP implementation of {@link IHopWebUrlUpdater}. Updates the browser URL when the user switches
 * tabs and builds shareable URLs for "Copy URL for this tab".
 */
public class RapHopWebUrlUpdater implements IHopWebUrlUpdater {

  @Override
  public void updateUrl(String projectName, String filePath) {
    String url = buildUrl(projectName, filePath);
    if (url == null) {
      return;
    }
    JavaScriptExecutor executor = RWT.getClient().getService(JavaScriptExecutor.class);
    String escaped = url.replace("\\", "\\\\").replace("'", "\\'");
    executor.execute("history.replaceState(null, '', '" + escaped + "');");
  }

  @Override
  public String buildUrl(String projectName, String filePath) {
    HttpServletRequest request = RWT.getRequest();
    if (request == null) {
      return null;
    }
    String base = request.getRequestURL().toString();
    StringBuilder sb = new StringBuilder(base);
    sb.append('?');
    if (projectName != null && !projectName.isEmpty()) {
      sb.append("project=").append(encode(projectName));
    }
    if (filePath != null && !filePath.isEmpty()) {
      if (sb.charAt(sb.length() - 1) != '?') {
        sb.append('&');
      }
      sb.append("file=").append(encode(filePath));
    }
    return sb.toString();
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  @Override
  public void copyToClipboard(String text) {
    if (text == null) {
      return;
    }
    // Escape for use inside a JavaScript string (single-quoted)
    String escaped =
        text.replace("\\", "\\\\").replace("'", "\\'").replace("\r", "\\r").replace("\n", "\\n");
    JavaScriptExecutor executor = RWT.getClient().getService(JavaScriptExecutor.class);
    String script =
        "(function(){ var t = '"
            + escaped
            + "';"
            + "if (navigator.clipboard && navigator.clipboard.writeText) {"
            + "navigator.clipboard.writeText(t).catch(function(){});"
            + "} else {"
            + "var ta = document.createElement('textarea'); ta.value = t;"
            + "ta.style.position = 'fixed'; ta.style.left = '-9999px';"
            + "document.body.appendChild(ta); ta.select();"
            + "try { document.execCommand('copy'); } catch(e) {}"
            + "document.body.removeChild(ta);"
            + "}})();";
    executor.execute(script);
  }
}
