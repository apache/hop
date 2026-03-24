/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui;

import org.eclipse.jface.text.source.SourceViewerConfiguration;
import org.eclipse.swt.widgets.Display;

/**
 * Builds a {@link SourceViewerConfiguration} for the content editor. Uses TM4E with TextMate
 * grammars for syntax highlighting of JSON, XML, and SQL; other languages get a plain config.
 */
public final class RuleBasedSourceViewerConfiguration {

  private RuleBasedSourceViewerConfiguration() {}

  /**
   * Creates a configuration for the given language. Uses TM4E when a grammar is available (json,
   * xml, sql); otherwise returns a plain configuration with no syntax highlighting.
   *
   * @param languageId language id (e.g. "json", "xml", "sql"), or null for plain text
   * @return configuration, never null
   */
  public static SourceViewerConfiguration create(String languageId) {
    if (languageId == null || languageId.isEmpty()) {
      return new SourceViewerConfiguration();
    }
    Display display = Display.getCurrent();
    if (display == null) {
      display = Display.getDefault();
    }
    SourceViewerConfiguration tm4eConfig =
        ContentEditorTm4eSupport.createConfiguration(languageId, display);
    if (tm4eConfig != null) {
      return tm4eConfig;
    }
    return new SourceViewerConfiguration();
  }
}
