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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.editor.IContentEditorWidget;
import org.eclipse.jface.text.DocumentEvent;
import org.eclipse.jface.text.IDocument;
import org.eclipse.jface.text.IDocumentExtension3;
import org.eclipse.jface.text.IDocumentListener;
import org.eclipse.jface.text.ITextViewerExtension5;
import org.eclipse.jface.text.Position;
import org.eclipse.jface.text.rules.FastPartitioner;
import org.eclipse.jface.text.rules.RuleBasedPartitionScanner;
import org.eclipse.jface.text.rules.Token;
import org.eclipse.jface.text.source.Annotation;
import org.eclipse.jface.text.source.AnnotationModel;
import org.eclipse.jface.text.source.AnnotationRulerColumn;
import org.eclipse.jface.text.source.CompositeRuler;
import org.eclipse.jface.text.source.IAnnotationAccess;
import org.eclipse.jface.text.source.IAnnotationAccessExtension;
import org.eclipse.jface.text.source.LineNumberRulerColumn;
import org.eclipse.jface.text.source.SourceViewer;
import org.eclipse.jface.text.source.SourceViewerConfiguration;
import org.eclipse.jface.text.source.projection.ProjectionAnnotation;
import org.eclipse.jface.text.source.projection.ProjectionAnnotationModel;
import org.eclipse.jface.text.source.projection.ProjectionViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CaretListener;
import org.eclipse.swt.custom.StyleRange;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.custom.VerifyKeyListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.PaintListener;
import org.eclipse.swt.events.VerifyEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;

/**
 * Desktop (RCP) implementation of the content editor using native SWT: Eclipse JFace SourceViewer
 * with rule-based syntax highlighting (XML, JSON, SQL), bracket matching, and XML auto-closing
 * tags.
 */
public class ContentEditorFacadeImpl extends ContentEditorFacade {

  @Override
  protected IContentEditorWidget createContentEditorInternal(Composite parent, String languageId) {
    Composite container = new Composite(parent, SWT.NONE);
    PropsUi.setLook(container);
    container.setLayout(new FormLayout());
    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(0, 0);
    fd.bottom = new FormAttachment(100, 0);
    container.setLayoutData(fd);

    int styles = SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL | SWT.BORDER;
    CompositeRuler ruler = new CompositeRuler();
    ProjectionViewer sourceViewer = new ProjectionViewer(container, ruler, null, false, styles);
    sourceViewer.setEditable(true);
    org.eclipse.jface.text.Document document = new org.eclipse.jface.text.Document();
    RuleBasedPartitionScanner partitionScanner = new RuleBasedPartitionScanner();
    partitionScanner.setDefaultReturnToken(new Token(IDocument.DEFAULT_CONTENT_TYPE));
    FastPartitioner partitioner = new FastPartitioner(partitionScanner, new String[0]);
    partitioner.connect(document);
    ((IDocumentExtension3) document)
        .setDocumentPartitioner(IDocumentExtension3.DEFAULT_PARTITIONING, partitioner);
    sourceViewer.setDocument(document, new AnnotationModel());

    SourceViewerConfiguration config = RuleBasedSourceViewerConfiguration.create(languageId);
    if (config != null) {
      sourceViewer.configure(config);
    }
    sourceViewer.invalidateTextPresentation();

    // Enable projection (folding) without ProjectionSupport so we control the ruler column
    sourceViewer.doOperation(ProjectionViewer.TOGGLE);

    // Line numbers first (index 0), then fold column (index 1) with explicit width
    LineNumberRulerColumn lineNumberColumn = new LineNumberRulerColumn();
    ruler.addDecorator(0, lineNumberColumn);

    ProjectionAnnotationModel projModel = sourceViewer.getProjectionAnnotationModel();
    if (projModel != null) {
      FoldingAnnotationAccess foldAccess = new FoldingAnnotationAccess();
      AnnotationRulerColumn foldColumn = new AnnotationRulerColumn(projModel, 20, foldAccess);
      foldColumn.addAnnotationType(ProjectionAnnotation.TYPE);
      ruler.addDecorator(1, foldColumn);

      Control foldControl = foldColumn.getControl();
      if (foldControl != null) {
        foldControl.addMouseListener(
            new MouseAdapter() {
              @Override
              public void mouseUp(MouseEvent e) {
                int line = foldColumn.toDocumentLineNumber(e.y);
                if (line < 0) return;
                IDocument doc = sourceViewer.getDocument();
                if (doc == null) return;
                java.util.Iterator<Annotation> it = projModel.getAnnotationIterator();
                while (it.hasNext()) {
                  Annotation ann = it.next();
                  if (ann instanceof ProjectionAnnotation) {
                    Position pos = projModel.getPosition(ann);
                    if (pos != null) {
                      try {
                        int annLine = doc.getLineOfOffset(pos.getOffset());
                        if (annLine == line) {
                          projModel.toggleExpansionState((ProjectionAnnotation) ann);
                          return;
                        }
                      } catch (org.eclipse.jface.text.BadLocationException ignored) {
                        // ignore
                      }
                    }
                  }
                }
              }
            });
      }
    }

    applyFontFromHop(sourceViewer);
    org.eclipse.swt.graphics.Font editorFont = sourceViewer.getTextWidget().getFont();
    if (editorFont != null && !editorFont.isDisposed()) {
      lineNumberColumn.setFont(editorFont);
    }
    boolean dark = PropsUi.getInstance().isDarkMode();
    if (dark) {
      org.eclipse.swt.graphics.Color lineNumFg =
          new org.eclipse.swt.graphics.Color(container.getDisplay(), 130, 130, 130);
      org.eclipse.swt.graphics.Color lineNumBg =
          new org.eclipse.swt.graphics.Color(container.getDisplay(), 40, 40, 40);
      lineNumberColumn.setForeground(lineNumFg);
      lineNumberColumn.setBackground(lineNumBg);
    } else {
      org.eclipse.swt.graphics.Color lineNumFg =
          new org.eclipse.swt.graphics.Color(container.getDisplay(), 120, 120, 120);
      lineNumberColumn.setForeground(lineNumFg);
    }
    sourceViewer.getTextWidget().setTabs(4);

    Control viewerControl = sourceViewer.getControl();
    FormData fdViewer = new FormData();
    fdViewer.left = new FormAttachment(0, 0);
    fdViewer.right = new FormAttachment(100, 0);
    fdViewer.top = new FormAttachment(0, 0);
    fdViewer.bottom = new FormAttachment(100, 0);
    viewerControl.setLayoutData(fdViewer);

    SwtContentEditorWidget widget = new SwtContentEditorWidget(container, sourceViewer, languageId);
    installBracketAndXmlSupport(widget);
    return widget;
  }

  /** Installs bracket highlighting and XML auto-closing when language is xml. */
  private static void installBracketAndXmlSupport(SwtContentEditorWidget widget) {
    widget.installBracketHighlighting();
    widget.installFolding();
    widget.installBlockGuidePainter();
    widget.installCollapsePlaceholderPainter();
    widget.installXmlAutoClose();
  }

  private static void applyFontFromHop(SourceViewer sourceViewer) {
    try {
      org.eclipse.swt.graphics.Font swtFont = GuiResource.getInstance().getFontFixed();
      if (swtFont != null && !swtFont.isDisposed()) {
        sourceViewer.getTextWidget().setFont(swtFont);
      }
    } catch (Exception e) {
      // GuiResource or font not available
    }
  }

  /** Kept for API compatibility; rule-based config uses languageId directly. */
  static String mapLanguageToScope(String languageId) {
    return languageId;
  }

  /**
   * Annotation access for the fold ruler column: paints chevron icons and reports annotation
   * metadata to the {@link AnnotationRulerColumn}.
   */
  private static class FoldingAnnotationAccess
      implements IAnnotationAccess, IAnnotationAccessExtension {

    @Override
    public Object getType(Annotation annotation) {
      return annotation.getType();
    }

    @Override
    public boolean isMultiLine(Annotation annotation) {
      return true;
    }

    @Override
    public boolean isTemporary(Annotation annotation) {
      return true;
    }

    @Override
    public String getTypeLabel(Annotation annotation) {
      return annotation.getText() != null ? annotation.getText() : "";
    }

    @Override
    public int getLayer(Annotation annotation) {
      return 0;
    }

    @Override
    public Object[] getSupertypes(Object annotationType) {
      return new Object[0];
    }

    @Override
    public boolean isSubtype(Object annotationType, Object potentialSupertype) {
      return annotationType != null && annotationType.equals(potentialSupertype);
    }

    @Override
    public boolean isPaintable(Annotation annotation) {
      return annotation instanceof ProjectionAnnotation;
    }

    @Override
    public void paint(Annotation annotation, GC gc, Canvas canvas, Rectangle rectangle) {
      if (!(annotation instanceof ProjectionAnnotation)) return;
      ProjectionAnnotation pa = (ProjectionAnnotation) annotation;
      boolean collapsed = pa.isCollapsed();
      int x = rectangle.x;
      int y = rectangle.y;
      int w = rectangle.width;
      int lineHeight = 14;
      int h = Math.min(rectangle.height, lineHeight);
      if (w < 5 || h < 5) return;

      gc.setForeground(canvas.getDisplay().getSystemColor(SWT.COLOR_WIDGET_FOREGROUND));
      gc.setAntialias(SWT.ON);
      gc.setLineWidth(1);
      int pad = 2;
      int cx = x + w / 2;
      int cy = y + h / 2;
      if (collapsed) {
        // Right-pointing chevron: >
        int left = x + pad;
        int right = x + w - pad;
        int top = y + pad;
        int bottom = y + h - pad;
        gc.drawLine(left, top, right, cy);
        gc.drawLine(right, cy, left, bottom);
      } else {
        // Down-pointing chevron: v
        int left = x + pad;
        int right = x + w - pad;
        int top = y + pad;
        int bottom = y + h - pad;
        gc.drawLine(left, top, cx, bottom);
        gc.drawLine(cx, bottom, right, top);
      }
    }
  }

  private static final class SwtContentEditorWidget implements IContentEditorWidget {

    private final Composite control;
    private final SourceViewer sourceViewer;
    private final List<ModifyListener> modifyListeners = new CopyOnWriteArrayList<>();
    private volatile boolean suppressModify;
    private volatile String languageId;

    SwtContentEditorWidget(Composite control, SourceViewer sourceViewer, String languageId) {
      this.control = control;
      this.sourceViewer = sourceViewer;
      this.languageId = languageId != null ? languageId : "";
      sourceViewer
          .getDocument()
          .addDocumentListener(
              new IDocumentListener() {
                @Override
                public void documentAboutToBeChanged(DocumentEvent event) {}

                @Override
                public void documentChanged(DocumentEvent event) {
                  fireModify();
                }

                private void fireModify() {
                  if (suppressModify) {
                    return;
                  }
                  Display display = control.getDisplay();
                  if (display == null || control.isDisposed()) {
                    return;
                  }
                  display.asyncExec(
                      () -> {
                        if (control.isDisposed() || suppressModify) {
                          return;
                        }
                        for (ModifyListener listener : new ArrayList<>(modifyListeners)) {
                          try {
                            listener.modifyText(null);
                          } catch (Exception ignored) {
                            // ignore
                          }
                        }
                      });
                }
              });
    }

    @Override
    public Control getControl() {
      return control;
    }

    @Override
    public String getText() {
      IDocument doc = sourceViewer.getDocument();
      return doc != null ? doc.get() : "";
    }

    @Override
    public void setText(String text) {
      doSetText(text != null ? text : "", false);
    }

    @Override
    public void setTextSuppressModify(String text) {
      doSetText(text != null ? text : "", true);
    }

    private void doSetText(String text, boolean suppress) {
      if (suppress) {
        suppressModify = true;
      }
      try {
        IDocument doc = sourceViewer.getDocument();
        if (doc != null) {
          doc.set(text);
        }
        sourceViewer.setSelectedRange(0, 0);
        sourceViewer.invalidateTextPresentation();
      } finally {
        if (suppress) {
          suppressModify = false;
        }
      }
    }

    @Override
    public void setLanguage(String languageId) {
      this.languageId = languageId != null ? languageId : "";
      SourceViewerConfiguration config = RuleBasedSourceViewerConfiguration.create(languageId);
      if (config != null) {
        sourceViewer.unconfigure();
        sourceViewer.configure(config);
        sourceViewer.invalidateTextPresentation();
      }
      updateFoldingRegions();
    }

    @Override
    public void setReadOnly(boolean readOnly) {
      sourceViewer.setEditable(!readOnly);
    }

    @Override
    public void addModifyListener(ModifyListener listener) {
      if (listener != null) {
        modifyListeners.add(listener);
      }
    }

    @Override
    public void removeModifyListener(ModifyListener listener) {
      if (listener != null) {
        modifyListeners.remove(listener);
      }
    }

    @Override
    public void selectAll() {
      IDocument doc = sourceViewer.getDocument();
      if (doc != null) {
        sourceViewer.setSelectedRange(0, doc.getLength());
      }
    }

    @Override
    public void unselectAll() {
      sourceViewer.setSelectedRange(0, 0);
    }

    @Override
    public void copy() {
      sourceViewer.doOperation(SourceViewer.COPY);
    }

    private static final char[] OPEN_BRACKETS = {'(', '[', '{'};
    private static final char[] CLOSE_BRACKETS = {')', ']', '}'};
    private int[] highlightedPositions;
    private List<ProjectionAnnotation> currentFoldAnnotations = new ArrayList<>();
    private Runnable pendingFoldUpdate;

    void installBracketHighlighting() {
      StyledText st = sourceViewer.getTextWidget();
      Color hlBg =
          new Color(
              st.getDisplay(),
              PropsUi.getInstance().isDarkMode()
                  ? new org.eclipse.swt.graphics.RGB(80, 80, 80)
                  : new org.eclipse.swt.graphics.RGB(220, 220, 220));
      st.addDisposeListener(e -> hlBg.dispose());

      CaretListener caretListener =
          event -> {
            clearBracketHighlight(st);
            String text = st.getText();
            int offset = event.caretOffset;
            int[] enclosing = findEnclosingBrackets(text, offset);
            if (enclosing != null) {
              applyBracketHighlight(st, enclosing[0], enclosing[1], hlBg);
            }
          };
      st.addCaretListener(caretListener);
    }

    private void clearBracketHighlight(StyledText st) {
      if (highlightedPositions == null) return;
      for (int pos : highlightedPositions) {
        if (pos < 0 || pos >= st.getCharCount()) continue;
        StyleRange existing = st.getStyleRangeAtOffset(pos);
        if (existing != null) {
          existing.background = null;
          st.setStyleRange(existing);
        }
      }
      highlightedPositions = null;
    }

    private void applyBracketHighlight(StyledText st, int pos1, int pos2, Color bg) {
      highlightedPositions = new int[] {pos1, pos2};
      for (int pos : highlightedPositions) {
        if (pos < 0 || pos >= st.getCharCount()) continue;
        StyleRange sr = st.getStyleRangeAtOffset(pos);
        if (sr == null) {
          sr = new StyleRange();
          sr.start = pos;
          sr.length = 1;
        } else {
          sr = (StyleRange) sr.clone();
        }
        sr.background = bg;
        st.setStyleRange(sr);
      }
    }

    /**
     * Finds the innermost enclosing bracket pair around the given offset. Scans forward from the
     * start of the text, tracking a bracket stack. The top of the stack at the cursor position is
     * the enclosing open bracket; its forward match gives the close bracket.
     *
     * @return int[]{openPos, closePos} or null if not inside any brackets
     */
    private static int[] findEnclosingBrackets(String text, int offset) {
      Deque<int[]> stack = new ArrayDeque<>();
      boolean inStr = false;

      for (int i = 0; i < offset && i < text.length(); i++) {
        char c = text.charAt(i);
        if (inStr) {
          if (c == '\\' && i + 1 < text.length()) {
            i++;
            continue;
          }
          if (c == '"') inStr = false;
          continue;
        }
        if (c == '"') {
          inStr = true;
          continue;
        }

        int openIdx = indexOf(OPEN_BRACKETS, c);
        if (openIdx >= 0) {
          stack.push(new int[] {i, openIdx});
        } else {
          int closeIdx = indexOf(CLOSE_BRACKETS, c);
          if (closeIdx >= 0 && !stack.isEmpty() && stack.peek()[1] == closeIdx) {
            stack.pop();
          }
        }
      }

      // Walk the stack from innermost (top) outward, find one whose close bracket is past offset
      while (!stack.isEmpty()) {
        int[] top = stack.pop();
        int openPos = top[0];
        int closePos =
            findMatchForward(text, openPos, OPEN_BRACKETS[top[1]], CLOSE_BRACKETS[top[1]]);
        if (closePos >= offset) {
          return new int[] {openPos, closePos};
        }
      }
      return null;
    }

    /** Finds the matching close bracket for an open bracket at pos, scanning forward. */
    private static int findMatchForward(String text, int pos, char open, char close) {
      int depth = 0;
      boolean inStr = false;
      for (int i = pos; i < text.length(); i++) {
        char c = text.charAt(i);
        if (inStr) {
          if (c == '\\' && i + 1 < text.length()) {
            i++;
            continue;
          }
          if (c == '"') inStr = false;
          continue;
        }
        if (c == '"') {
          inStr = true;
          continue;
        }
        if (c == open) depth++;
        else if (c == close) depth--;
        if (depth == 0) return i;
      }
      return -1;
    }

    private static int indexOf(char[] arr, char c) {
      for (int i = 0; i < arr.length; i++) {
        if (arr[i] == c) return i;
      }
      return -1;
    }

    void installFolding() {
      if (!(sourceViewer instanceof ProjectionViewer)) return;

      sourceViewer
          .getDocument()
          .addDocumentListener(
              new IDocumentListener() {
                @Override
                public void documentAboutToBeChanged(DocumentEvent event) {}

                @Override
                public void documentChanged(DocumentEvent event) {
                  scheduleFoldUpdate();
                }
              });
    }

    /** Paints vertical block guide lines (indent/bracket scope) like VS Code/IntelliJ. */
    void installBlockGuidePainter() {
      StyledText st = sourceViewer.getTextWidget();
      boolean dark = PropsUi.getInstance().isDarkMode();
      org.eclipse.swt.graphics.Color guideColor =
          new org.eclipse.swt.graphics.Color(
              st.getDisplay(), dark ? 60 : 200, dark ? 60 : 200, dark ? 60 : 200);
      st.addDisposeListener(e -> guideColor.dispose());

      final int guideWidthPx = 10;
      final boolean useModelMapping = sourceViewer instanceof ITextViewerExtension5;

      st.addPaintListener(
          new PaintListener() {
            @Override
            public void paintControl(PaintEvent e) {
              if (!"json".equalsIgnoreCase(languageId) && !"xml".equalsIgnoreCase(languageId)) {
                return;
              }
              String text = getText();
              if (text.isEmpty()) return;

              List<Position> regions =
                  "json".equalsIgnoreCase(languageId)
                      ? computeJsonFoldRegions(text)
                      : computeXmlFoldRegions(text);
              if (regions.isEmpty()) return;

              // Convert to (startLine+1, closingLine) blocks — guides show between open/close,
              // not on the opening or closing line itself.
              // Column = nesting depth (how many other blocks contain this one).
              List<int[]> rawBlocks = new ArrayList<>();
              for (Position pos : regions) {
                int startLine = lineOfOffset(text, pos.offset);
                int closingLine = lineOfOffset(text, pos.offset + pos.length - 1);
                if (closingLine > startLine + 1) {
                  rawBlocks.add(new int[] {startLine + 1, closingLine});
                }
              }
              // Deduplicate identical ranges
              Map<Long, int[]> uniqueBlocks = new HashMap<>();
              for (int[] b : rawBlocks) {
                long key = ((long) b[0] << 32) | (b[1] & 0xFFFFFFFFL);
                uniqueBlocks.putIfAbsent(key, b);
              }
              // Compute depth for each block: count how many other blocks strictly contain it
              List<int[]> blocks = new ArrayList<>();
              for (int[] b : uniqueBlocks.values()) {
                int depth = 0;
                for (int[] other : uniqueBlocks.values()) {
                  if (other[0] <= b[0] && other[1] >= b[1] && other != b) {
                    depth++;
                  }
                }
                blocks.add(new int[] {b[0], b[1], depth});
              }

              int lineHeight = st.getLineHeight();
              int topIndex = st.getTopIndex();
              int visibleCount =
                  Math.min(
                      st.getLineCount() - topIndex,
                      (st.getClientArea().height + lineHeight - 1) / lineHeight);

              e.gc.setForeground(guideColor);
              e.gc.setLineWidth(1);
              e.gc.setLineStyle(SWT.LINE_SOLID);

              ITextViewerExtension5 ext5 =
                  useModelMapping ? (ITextViewerExtension5) sourceViewer : null;

              for (int i = 0; i < visibleCount; i++) {
                int widgetLine = topIndex + i;
                int modelLine = (ext5 != null) ? ext5.widgetLine2ModelLine(widgetLine) : widgetLine;
                if (modelLine < 0) continue;
                int y = i * lineHeight;
                for (int[] block : blocks) {
                  int start = block[0];
                  int endExcl = block[1];
                  int col = block[2];
                  if (modelLine >= start && modelLine < endExcl) {
                    int x = 2 + col * guideWidthPx;
                    e.gc.drawLine(x, y, x, y + lineHeight);
                  }
                }
              }
            }
          });
    }

    /** 0-based line number for the given offset (counts '\n' before offset). */
    private static int lineOfOffset(String text, int offset) {
      int line = 0;
      for (int i = 0; i < offset && i < text.length(); i++) {
        if (text.charAt(i) == '\n') line++;
      }
      return line;
    }

    /** Paints "..." at the end of the first line of each collapsed fold (like VS Code). */
    void installCollapsePlaceholderPainter() {
      if (!(sourceViewer instanceof ProjectionViewer)) return;
      if (!(sourceViewer instanceof ITextViewerExtension5)) return;

      StyledText st = sourceViewer.getTextWidget();
      boolean dark = PropsUi.getInstance().isDarkMode();
      org.eclipse.swt.graphics.Color placeholderColor =
          new org.eclipse.swt.graphics.Color(
              st.getDisplay(), dark ? 140 : 120, dark ? 140 : 120, dark ? 140 : 120);
      st.addDisposeListener(e -> placeholderColor.dispose());

      st.addPaintListener(
          e1 -> {
            ProjectionViewer pv = (ProjectionViewer) sourceViewer;
            ProjectionAnnotationModel model = pv.getProjectionAnnotationModel();
            if (model == null) return;
            IDocument doc = sourceViewer.getDocument();
            if (doc == null) return;
            ITextViewerExtension5 ext5 = (ITextViewerExtension5) sourceViewer;

            e1.gc.setForeground(placeholderColor);
            e1.gc.setFont(st.getFont());

            java.util.Iterator<Annotation> it = model.getAnnotationIterator();
            while (it.hasNext()) {
              Annotation ann = it.next();
              if (!(ann instanceof ProjectionAnnotation)) continue;
              if (!((ProjectionAnnotation) ann).isCollapsed()) continue;
              Position pos = model.getPosition(ann);
              if (pos == null) continue;
              int modelLine;
              try {
                modelLine = doc.getLineOfOffset(pos.offset);
              } catch (org.eclipse.jface.text.BadLocationException ex) {
                continue;
              }
              int widgetLine = ext5.modelLine2WidgetLine(modelLine);
              if (widgetLine < 0 || widgetLine >= st.getLineCount()) continue;
              int lineEndOffset;
              try {
                int lineStart = st.getOffsetAtLine(widgetLine);
                int lineLen = st.getLine(widgetLine).length();
                lineEndOffset = lineStart + lineLen;
              } catch (Exception ex) {
                continue;
              }
              org.eclipse.swt.graphics.Point pt = st.getLocationAtOffset(lineEndOffset);
              if (pt != null) {
                e1.gc.drawText("...", pt.x + 4, pt.y);
              }
            }
          });

      // Click on "..." placeholder expands the collapsed fold
      final int placeholderClickWidth = 24;
      st.addMouseListener(
          new org.eclipse.swt.events.MouseAdapter() {
            @Override
            public void mouseUp(org.eclipse.swt.events.MouseEvent e) {
              if (e.button != 1) return;
              ProjectionViewer pv = (ProjectionViewer) sourceViewer;
              ProjectionAnnotationModel model = pv.getProjectionAnnotationModel();
              if (model == null) return;
              IDocument doc = sourceViewer.getDocument();
              if (doc == null) return;
              ITextViewerExtension5 ext5 = (ITextViewerExtension5) sourceViewer;

              int lineHeight = st.getLineHeight();
              java.util.Iterator<Annotation> it = model.getAnnotationIterator();
              while (it.hasNext()) {
                Annotation ann = it.next();
                if (!(ann instanceof ProjectionAnnotation)) continue;
                if (!((ProjectionAnnotation) ann).isCollapsed()) continue;
                Position pos = model.getPosition(ann);
                if (pos == null) continue;
                int modelLine;
                try {
                  modelLine = doc.getLineOfOffset(pos.offset);
                } catch (org.eclipse.jface.text.BadLocationException ex) {
                  continue;
                }
                int widgetLine = ext5.modelLine2WidgetLine(modelLine);
                if (widgetLine < 0 || widgetLine >= st.getLineCount()) continue;
                int lineEndOffset;
                try {
                  int lineStart = st.getOffsetAtLine(widgetLine);
                  int lineLen = st.getLine(widgetLine).length();
                  lineEndOffset = lineStart + lineLen;
                } catch (Exception ex) {
                  continue;
                }
                org.eclipse.swt.graphics.Point pt = st.getLocationAtOffset(lineEndOffset);
                if (pt == null) continue;
                int x1 = pt.x + 4;
                int y1 = pt.y;
                if (e.x >= x1
                    && e.x <= x1 + placeholderClickWidth
                    && e.y >= y1
                    && e.y < y1 + lineHeight) {
                  model.expand(ann);
                  return;
                }
              }
            }
          });
    }

    private void scheduleFoldUpdate() {
      Display display = control.getDisplay();
      if (display == null || display.isDisposed()) return;
      if (pendingFoldUpdate != null) {
        display.timerExec(-1, pendingFoldUpdate);
      }
      pendingFoldUpdate =
          () -> {
            if (!control.isDisposed()) {
              updateFoldingRegions();
            }
            pendingFoldUpdate = null;
          };
      display.timerExec(500, pendingFoldUpdate);
    }

    private void updateFoldingRegions() {
      if (!(sourceViewer instanceof ProjectionViewer)) return;
      ProjectionViewer pv = (ProjectionViewer) sourceViewer;
      ProjectionAnnotationModel model = pv.getProjectionAnnotationModel();
      if (model == null) return;

      if (pendingFoldUpdate != null && !control.isDisposed()) {
        control.getDisplay().timerExec(-1, pendingFoldUpdate);
        pendingFoldUpdate = null;
      }

      String text = getText();
      List<Position> regions;
      if ("json".equalsIgnoreCase(languageId)) {
        regions = computeJsonFoldRegions(text);
      } else if ("xml".equalsIgnoreCase(languageId)) {
        regions = computeXmlFoldRegions(text);
      } else {
        regions = List.of();
      }

      Annotation[] deletions = currentFoldAnnotations.toArray(new Annotation[0]);
      Map<ProjectionAnnotation, Position> additions = new HashMap<>();
      List<ProjectionAnnotation> newAnnotations = new ArrayList<>();
      for (Position pos : regions) {
        ProjectionAnnotation annotation = new ProjectionAnnotation();
        additions.put(annotation, pos);
        newAnnotations.add(annotation);
      }

      model.modifyAnnotations(deletions, additions, null);
      currentFoldAnnotations = newAnnotations;
    }

    private static List<Position> computeJsonFoldRegions(String text) {
      List<Position> regions = new ArrayList<>();
      Deque<Integer> stack = new ArrayDeque<>();
      boolean inString = false;

      for (int i = 0; i < text.length(); i++) {
        char c = text.charAt(i);
        if (inString) {
          if (c == '\\' && i + 1 < text.length()) {
            i++;
            continue;
          }
          if (c == '"') inString = false;
          continue;
        }
        if (c == '"') {
          inString = true;
          continue;
        }
        if (c == '{' || c == '[') {
          stack.push(i);
        } else if (c == '}' || c == ']') {
          if (!stack.isEmpty()) {
            int start = stack.pop();
            if (spanMultipleLines(text, start, i)) {
              regions.add(new Position(start, i - start + 1));
            }
          }
        }
      }
      return regions;
    }

    private static List<Position> computeXmlFoldRegions(String text) {
      List<Position> regions = new ArrayList<>();

      // Fold multi-line comments
      int idx = 0;
      while (idx < text.length()) {
        int cs = text.indexOf("<!--", idx);
        if (cs < 0) break;
        int ce = text.indexOf("-->", cs + 4);
        if (ce < 0) break;
        ce += 3;
        if (spanMultipleLines(text, cs, ce)) {
          regions.add(new Position(cs, ce - cs));
        }
        idx = ce;
      }

      // Fold multi-line CDATA
      idx = 0;
      while (idx < text.length()) {
        int cs = text.indexOf("<![CDATA[", idx);
        if (cs < 0) break;
        int ce = text.indexOf("]]>", cs + 9);
        if (ce < 0) break;
        ce += 3;
        if (spanMultipleLines(text, cs, ce)) {
          regions.add(new Position(cs, ce - cs));
        }
        idx = ce;
      }

      // Fold matching open/close tags
      Deque<String> nameStack = new ArrayDeque<>();
      Deque<Integer> posStack = new ArrayDeque<>();
      int i = 0;
      while (i < text.length()) {
        if (text.charAt(i) != '<') {
          i++;
          continue;
        }
        if (text.startsWith("<!--", i) || text.startsWith("<![CDATA[", i)) {
          int skip =
              text.startsWith("<!--", i) ? text.indexOf("-->", i + 4) : text.indexOf("]]>", i + 9);
          i = (skip < 0) ? text.length() : skip + 3;
          continue;
        }
        if (text.startsWith("<?", i) || text.startsWith("<!", i)) {
          int gt = text.indexOf('>', i);
          i = (gt < 0) ? text.length() : gt + 1;
          continue;
        }

        // Close tag
        if (text.startsWith("</", i)) {
          int gt = text.indexOf('>', i);
          if (gt < 0) break;
          String closeName = text.substring(i + 2, gt).trim();
          Deque<String> tmpNames = new ArrayDeque<>();
          Deque<Integer> tmpPos = new ArrayDeque<>();
          boolean found = false;
          while (!nameStack.isEmpty()) {
            String n = nameStack.pop();
            int p = posStack.pop();
            if (n.equals(closeName)) {
              int end = gt + 1;
              if (spanMultipleLines(text, p, end)) {
                regions.add(new Position(p, end - p));
              }
              found = true;
              break;
            }
            tmpNames.push(n);
            tmpPos.push(p);
          }
          if (!found) {
            while (!tmpNames.isEmpty()) {
              nameStack.push(tmpNames.pop());
              posStack.push(tmpPos.pop());
            }
          }
          i = gt + 1;
          continue;
        }

        // Open tag
        int gt = text.indexOf('>', i);
        if (gt < 0) break;
        boolean selfClosing = text.charAt(gt - 1) == '/';
        if (!selfClosing) {
          int nameStart = i + 1;
          int nameEnd = nameStart;
          while (nameEnd < text.length()) {
            char ch = text.charAt(nameEnd);
            if (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '>' || ch == '/')
              break;
            nameEnd++;
          }
          String tagName = text.substring(nameStart, nameEnd);
          if (!tagName.isEmpty()) {
            nameStack.push(tagName);
            posStack.push(i);
          }
        }
        i = gt + 1;
      }

      return regions;
    }

    private static boolean spanMultipleLines(String text, int start, int end) {
      for (int i = start; i < end && i < text.length(); i++) {
        if (text.charAt(i) == '\n') return true;
      }
      return false;
    }

    /**
     * When language is XML, typing '>' after an open tag inserts the closing tag and places the
     * caret between '>' and '</tagname>'.
     */
    void installXmlAutoClose() {
      if (!(sourceViewer instanceof org.eclipse.jface.text.ITextViewerExtension)) {
        return;
      }
      ((org.eclipse.jface.text.ITextViewerExtension) sourceViewer)
          .appendVerifyKeyListener(
              new VerifyKeyListener() {
                @Override
                public void verifyKey(VerifyEvent event) {
                  if (event.character != '>' || !"xml".equalsIgnoreCase(languageId)) {
                    return;
                  }
                  IDocument doc = sourceViewer.getDocument();
                  if (doc == null) return;
                  int offset = sourceViewer.getSelectedRange().x;
                  String before = "";
                  try {
                    before = offset > 0 ? doc.get(0, offset) : "";
                  } catch (org.eclipse.jface.text.BadLocationException e) {
                    return;
                  }
                  String tagName = findOpenTagNameBefore(before);
                  if (tagName == null) return;
                  event.doit = false;
                  try {
                    doc.replace(offset, 0, "></" + tagName + ">");
                    sourceViewer.setSelectedRange(offset + 1, 0);
                  } catch (org.eclipse.jface.text.BadLocationException e) {
                    // ignore
                  }
                }

                private String findOpenTagNameBefore(String text) {
                  int i = text.length() - 1;
                  while (i >= 0 && text.charAt(i) != '<') i--;
                  if (i < 0) return null;
                  if (i + 1 < text.length()) {
                    char next = text.charAt(i + 1);
                    if (next == '/' || next == '?' || next == '!') return null;
                  }
                  StringBuilder name = new StringBuilder();
                  for (i = i + 1; i < text.length(); i++) {
                    char ch = text.charAt(i);
                    if (ch == ' ' || ch == '\t' || ch == '>' || ch == '/') break;
                    if (Character.isLetterOrDigit(ch)
                        || ch == ':'
                        || ch == '-'
                        || ch == '_'
                        || ch == '.') {
                      name.append(ch);
                    } else {
                      break;
                    }
                  }
                  return name.length() > 0 ? name.toString() : null;
                }
              });
    }
  }
}
