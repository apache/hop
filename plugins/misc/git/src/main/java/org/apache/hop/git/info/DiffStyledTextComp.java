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

  private final Color colorAddition;
  private final Color colorDeletion;
  private final Color colorFileHeader;
  private final Color colorHunkHeader;

  public DiffStyledTextComp(IVariables variables, Composite parent, int style) {
    super(variables, parent, style, false, false, true, STYLE_TYPE_DIFF); // No variable support
    // Set read-only and disable editing
    getTextWidget().setEditable(false);
    Display display = parent.getDisplay();
    colorAddition = new Color(display, 0, 128, 0);
    colorDeletion = new Color(display, 255, 0, 0);
    colorFileHeader = new Color(display, 0, 128, 128);
    colorHunkHeader = new Color(display, 153, 102, 0);
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
        styleRange = new StyleRange(offset, lineLength, colorAddition, null);
      } else if (line.startsWith("-")) {
        // Deletion line (red)
        styleRange = new StyleRange(offset, lineLength, colorDeletion, null);
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
        styleRange = new StyleRange(offset, lineLength, colorFileHeader, null);
        if (line.startsWith("+++") || line.startsWith("---")) {
          styleRange.fontStyle = SWT.BOLD;
        }
      } else if (line.startsWith("@@")) {
        // Hunk header (brown/orange)
        styleRange = new StyleRange(offset, lineLength, colorHunkHeader, null);
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
    colorAddition.dispose();
    colorDeletion.dispose();
    colorFileHeader.dispose();
    colorHunkHeader.dispose();
    super.dispose();
  }
}
