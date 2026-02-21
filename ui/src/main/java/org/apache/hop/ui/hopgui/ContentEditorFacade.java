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

import org.apache.hop.ui.core.widget.editor.IContentEditorWidget;
import org.eclipse.swt.widgets.Composite;

/**
 * Facade for creating a content/code editor widget. Abstracts the difference between desktop (e.g.
 * RSyntaxTextArea) and Hop Web (e.g. Monaco or plain text).
 *
 * <p>Use {@link #createContentEditor(Composite, String)} to obtain an editor that supports {@link
 * IContentEditorWidget#setText(String)}, {@link IContentEditorWidget#getText()}, and {@link
 * IContentEditorWidget#setLanguage(String)} through a consistent interface.
 */
public abstract class ContentEditorFacade {

  private static final ContentEditorFacade IMPL;

  static {
    IMPL = (ContentEditorFacade) ImplementationLoader.newInstance(ContentEditorFacade.class);
  }

  /**
   * Create a content editor widget as a child of the given composite.
   *
   * @param parent the parent composite
   * @param languageId language for syntax highlighting (e.g. "json", "xml"), or null for plain text
   * @return an editor widget with a consistent getText/setText/setLanguage API
   */
  public static IContentEditorWidget createContentEditor(Composite parent, String languageId) {
    return IMPL.createContentEditorInternal(parent, languageId);
  }

  /**
   * Implementation-specific editor creation.
   *
   * @param parent the parent composite
   * @param languageId language for syntax highlighting, or null for plain text
   * @return the editor widget
   */
  protected abstract IContentEditorWidget createContentEditorInternal(
      Composite parent, String languageId);
}
