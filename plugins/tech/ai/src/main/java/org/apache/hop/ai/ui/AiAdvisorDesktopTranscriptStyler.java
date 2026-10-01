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

package org.apache.hop.ai.ui;

import org.apache.hop.ai.engine.AiAdvisorMarkdown;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StyleRange;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;

/**
 * Desktop-only markdown transcript renderer using {@link StyledText} and {@link StyleRange}.
 * Isolated into a separate class so {@link AiAdvisorTranscriptPanel} can be loaded in Hop Web
 * (RAP/RWT) without triggering a {@link NoClassDefFoundError} on desktop-only SWT classes.
 */
class AiAdvisorDesktopTranscriptStyler {

  private AiAdvisorDesktopTranscriptStyler() {}

  static Control createAssistantBody(
      Composite block, GridData gd, AiAdvisorTranscriptPanel.Role role, String text) {
    StyledText body = new StyledText(block, SWT.MULTI | SWT.WRAP | SWT.READ_ONLY);
    AiAdvisorMarkdown.Document document = AiAdvisorMarkdown.render(text);
    body.setText(document.text());
    body.setLayoutData(gd);
    AiAdvisorTranscriptPanel.applyRoleLook(body, role);
    applyMarkdownStyles(body, document);
    return body;
  }

  private static void applyMarkdownStyles(StyledText widget, AiAdvisorMarkdown.Document document) {
    GuiResource gui = GuiResource.getInstance();
    for (AiAdvisorMarkdown.Span span : document.spans()) {
      StyleRange range = new StyleRange();
      range.start = span.start();
      range.length = span.length();
      switch (span.kind()) {
        case HEADING -> range.font = gui.getFontMediumBold();
        case BOLD -> range.fontStyle = SWT.BOLD;
        case EMPHASIS -> range.fontStyle = SWT.ITALIC;
        case CODE -> range.font = gui.getFontFixed();
      }
      widget.setStyleRange(range);
    }
  }

  static int lineHeight(Control body) {
    if (body instanceof StyledText styled) {
      return styled.getLineHeight();
    }
    return 16;
  }
}
