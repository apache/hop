/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.git.info;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.core.widget.StyledTextVar;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StyleRange;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;

/**
 * Styled text component for displaying git diffs with syntax highlighting. Green for additions (+),
 * red for deletions (-), cyan for file headers (diff --git, +++, ---), and yellow for hunk headers
 * (@@).
 *
 * <p>This class should only be used in desktop mode, not in Hop Web.
 */
public class DiffStyledTextComp extends StyledTextVar {

  private static final Color COLOR_ADDITION = new Color(Display.getDefault(), 0, 128, 0); // Green
  private static final Color COLOR_DELETION = new Color(Display.getDefault(), 255, 0, 0); // Red
  private static final Color COLOR_FILE_HEADER =
      new Color(Display.getDefault(), 0, 128, 128); // Cyan
  private static final Color COLOR_HUNK_HEADER =
      new Color(Display.getDefault(), 153, 102, 0); // Brown/Orange

  public DiffStyledTextComp(IVariables variables, Composite parent, int style) {
    super(variables, parent, style, false, false); // No variable support needed
    // Set read-only and disable editing
    getTextWidget().setEditable(false);
  }

  /**
   * Sets the diff text and applies syntax highlighting based on git diff format.
   *
   * @param diffText The git diff text to display
   */
  public void setDiffText(String diffText) {
    if (diffText == null || diffText.isEmpty()) {
      setText("");
      return;
    }

    setText(diffText);
    applyDiffHighlighting();
  }

  /** Applies syntax highlighting to the diff text based on git diff format. */
  private void applyDiffHighlighting() {
    StyledText styledText = getTextWidget();
    String text = styledText.getText();
    if (text.isEmpty()) {
      return;
    }

    String[] lines = text.split("\n", -1);
    int offset = 0;

    for (String line : lines) {
      int lineLength = line.length();
      StyleRange styleRange = null;

      if (line.startsWith("+")) {
        // Addition line (green)
        styleRange = new StyleRange(offset, lineLength, COLOR_ADDITION, null);
      } else if (line.startsWith("-")) {
        // Deletion line (red)
        styleRange = new StyleRange(offset, lineLength, COLOR_DELETION, null);
      } else if (line.startsWith("diff --git")
          || line.startsWith("+++")
          || line.startsWith("---")
          || line.startsWith("index ")
          || line.startsWith("new file")
          || line.startsWith("deleted file")
          || line.startsWith("similarity index")
          || line.startsWith("rename from")
          || line.startsWith("rename to")) {
        // File header (cyan)
        styleRange = new StyleRange(offset, lineLength, COLOR_FILE_HEADER, null);
        if (line.startsWith("+++") || line.startsWith("---")) {
          styleRange.fontStyle = SWT.BOLD;
        }
      } else if (line.startsWith("@@")) {
        // Hunk header (brown/orange)
        styleRange = new StyleRange(offset, lineLength, COLOR_HUNK_HEADER, null);
        styleRange.fontStyle = SWT.BOLD;
      }

      if (styleRange != null) {
        styledText.setStyleRange(styleRange);
      }

      // Move offset to next line (including newline character)
      offset += lineLength + 1;
    }
  }

  @Override
  public void dispose() {
    // Colors are shared and managed by Display, no need to dispose them explicitly
    super.dispose();
  }
}
