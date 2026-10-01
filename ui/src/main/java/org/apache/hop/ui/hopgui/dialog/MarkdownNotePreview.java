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

package org.apache.hop.ui.hopgui.dialog;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.gui.markdown.CommonMarkConfig;
import org.apache.hop.core.gui.markdown.NoteImageSupport;
import org.apache.hop.core.util.TempFileUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.ShowBrowserDialog;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.widgets.Shell;

/**
 * Shows an HTML preview of Markdown note source (dialog / external browser), reusing the same
 * CommonMark extensions as canvas rendering. Relative image paths are expanded to absolute {@code
 * file:} URLs so a browser can load them.
 */
public final class MarkdownNotePreview {

  private static final Class<?> PKG = NotePadDialog.class;

  private static final Pattern IMG_SRC =
      Pattern.compile("(?i)(<img\\b[^>]*\\bsrc\\s*=\\s*)([\"'])([^\"']+)\\2");

  private MarkdownNotePreview() {}

  public static void show(
      Shell shell, String markdownSource, IVariables variables, String baseFilename) {
    try {
      String body = CommonMarkConfig.toHtmlBody(markdownSource != null ? markdownSource : "");
      body = expandImageSources(body, variables, baseFilename);
      String fullHtml = wrapDocument(body, PropsUi.getInstance().isDarkMode());

      if (EnvironmentUtils.getInstance().isWeb()) {
        ShowBrowserDialog dialog =
            new ShowBrowserDialog(
                shell,
                BaseMessages.getString(PKG, "NotePadDialog.Markdown.Preview.Title"),
                fullHtml);
        dialog.open();
      } else {
        File tempFile = TempFileUtil.createTempFileObject("hop_note_md_preview_", ".html");
        tempFile.deleteOnExit();
        try (OutputStream out = new FileOutputStream(tempFile)) {
          out.write(fullHtml.getBytes(StandardCharsets.UTF_8));
        }
        EnvironmentUtils.getInstance().openUrl(tempFile.toURI().toString());
      }
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "NotePadDialog.Markdown.Preview.Error.Title"),
          BaseMessages.getString(PKG, "NotePadDialog.Markdown.Preview.Error.Message"),
          e);
    }
  }

  /**
   * Rewrite {@code <img src="...">} values to absolute browser-loadable URLs when they are local
   * relative (or absolute file) paths.
   */
  static String expandImageSources(String html, IVariables variables, String baseFilename) {
    if (Utils.isEmpty(html)) {
      return html;
    }
    Matcher matcher = IMG_SRC.matcher(html);
    StringBuilder out = new StringBuilder();
    while (matcher.find()) {
      String prefix = matcher.group(1);
      String quote = matcher.group(2);
      String src = matcher.group(3);
      String expanded = toBrowserImageUrl(variables, baseFilename, src);
      matcher.appendReplacement(out, Matcher.quoteReplacement(prefix + quote + expanded + quote));
    }
    matcher.appendTail(out);
    return out.toString();
  }

  static String toBrowserImageUrl(IVariables variables, String baseFilename, String src) {
    if (Utils.isEmpty(src)) {
      return src;
    }
    String trimmed = src.trim();
    // Already a network or data URL — leave as-is
    if (NoteImageSupport.isNetworkUrl(trimmed)
        || trimmed.regionMatches(true, 0, "data:", 0, 5)
        || trimmed.regionMatches(true, 0, "blob:", 0, 5)) {
      return trimmed;
    }
    try {
      String resolved = NoteImageSupport.resolvePath(variables, baseFilename, trimmed);
      if (Utils.isEmpty(resolved)) {
        return trimmed;
      }
      FileObject file = HopVfs.getFileObject(resolved, variables);
      // Prefer a file: URL the system browser can open for local files
      try {
        java.net.URL url = file.getURL();
        if (url != null) {
          return url.toExternalForm();
        }
      } catch (Exception ignored) {
        // fall through
      }
      String uri = file.getName().getURI();
      return uri != null ? uri : trimmed;
    } catch (Exception e) {
      return trimmed;
    }
  }

  static String wrapDocument(String bodyHtml, boolean darkMode) {
    StringBuilder html = new StringBuilder();
    html.append("<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"UTF-8\">\n<style>\n");
    html.append("body { font-family: system-ui, sans-serif; line-height: 1.5; margin: 24px; }\n");
    html.append("h1, h2, h3 { font-weight: 600; }\n");
    html.append("pre, code { font-family: ui-monospace, monospace; font-size: 0.9em; }\n");
    html.append("pre { padding: 12px; overflow-x: auto; border-radius: 6px; }\n");
    html.append("code { padding: 0.15em 0.35em; border-radius: 4px; }\n");
    html.append("table { border-collapse: collapse; margin: 1em 0; width: 100%; }\n");
    html.append("th, td { border: 1px solid; padding: 0.45em 0.7em; text-align: left; }\n");
    html.append("th { font-weight: 600; }\n");
    html.append("img { max-width: 100%; height: auto; }\n");
    html.append("a { text-decoration: none; }\n");
    html.append("a:hover { text-decoration: underline; }\n");
    if (darkMode) {
      html.append("body { background: #0b0f19; color: #e2e8f0; }\n");
      html.append("pre, code { background: #1e293b; }\n");
      html.append("th, td { border-color: #334155; }\n");
      html.append("th { background: #1e293b; }\n");
      html.append("a { color: #7dd3fc; }\n");
    } else {
      html.append("body { background: #ffffff; color: #0f172a; }\n");
      html.append("pre, code { background: #f1f5f9; }\n");
      html.append("th, td { border-color: #e2e8f0; }\n");
      html.append("th { background: #f8fafc; }\n");
      html.append("a { color: #0369a1; }\n");
    }
    html.append("</style>\n</head>\n<body>\n");
    html.append(bodyHtml != null ? bodyHtml : "");
    html.append("\n</body>\n</html>");
    return html.toString();
  }
}
