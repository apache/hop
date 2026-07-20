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
 * Desktop (RCP) implementation of the content editor using native SWT: Eclipse JFace SourceViewer
 * with rule-based syntax highlighting (XML, JSON, SQL), bracket matching, and XML auto-closing
 * tags.
 */
public class ContentEditorFacadeImpl extends ContentEditorFacade {
  @Override
  protected IContentEditorWidget createContentEditorInternal(Composite parent, String languageId) {
    return new ContentEditorWidget(parent, languageId);
  }
}
