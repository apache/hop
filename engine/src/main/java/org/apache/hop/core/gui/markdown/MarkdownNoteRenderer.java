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

package org.apache.hop.core.gui.markdown;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.NotePadType;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.NotePadStyle;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.commonmark.ext.gfm.tables.TableBlock;
import org.commonmark.ext.gfm.tables.TableCell;
import org.commonmark.ext.gfm.tables.TableHead;
import org.commonmark.ext.gfm.tables.TableRow;
import org.commonmark.ext.task.list.items.TaskListItemMarker;
import org.commonmark.node.AbstractVisitor;
import org.commonmark.node.BulletList;
import org.commonmark.node.Code;
import org.commonmark.node.CustomBlock;
import org.commonmark.node.CustomNode;
import org.commonmark.node.Document;
import org.commonmark.node.Emphasis;
import org.commonmark.node.FencedCodeBlock;
import org.commonmark.node.HardLineBreak;
import org.commonmark.node.Heading;
import org.commonmark.node.Image;
import org.commonmark.node.IndentedCodeBlock;
import org.commonmark.node.Link;
import org.commonmark.node.ListItem;
import org.commonmark.node.Node;
import org.commonmark.node.OrderedList;
import org.commonmark.node.Paragraph;
import org.commonmark.node.SoftLineBreak;
import org.commonmark.node.StrongEmphasis;
import org.commonmark.node.Text;
import org.commonmark.node.ThematicBreak;

/**
 * Lays out and paints CommonMark/GFM Markdown note content onto an {@link IGc}. Produces link hit
 * rectangles for graph interaction when painting.
 */
public final class MarkdownNoteRenderer {

  private static final int DEFAULT_FONT_SIZE = 10;
  private static final int CELL_PAD = 4;
  private static final int BLOCK_GAP = 4;
  private static final int LIST_INDENT = 14;

  private MarkdownNoteRenderer() {}

  public record LayoutResult(int width, int height, List<NoteLinkHit> links) {}

  /**
   * Measure natural content size (no wrapping beyond a generous max) for note minimum size.
   *
   * @param contentWidth when &gt; 0, wrap text to this width; otherwise use natural line widths
   */
  public static LayoutResult measure(
      IGc gc,
      NotePadMeta note,
      String fontName,
      int baseFontSize,
      int contentWidth,
      String baseFilename,
      IVariables variables) {
    return render(
        gc, note, fontName, baseFontSize, 0, 0, contentWidth, false, null, baseFilename, variables);
  }

  /**
   * Paint markdown content and return measured size plus link hits (coordinates relative to gc).
   */
  public static LayoutResult paint(
      IGc gc,
      NotePadMeta note,
      String fontName,
      int baseFontSize,
      int originX,
      int originY,
      int contentWidth,
      NoteLinkHit hoverLink,
      String baseFilename,
      IVariables variables) {
    return render(
        gc,
        note,
        fontName,
        baseFontSize,
        originX,
        originY,
        contentWidth,
        true,
        hoverLink,
        baseFilename,
        variables);
  }

  private static LayoutResult render(
      IGc gc,
      NotePadMeta note,
      String fontName,
      int baseFontSize,
      int originX,
      int originY,
      int contentWidth,
      boolean paint,
      NoteLinkHit hoverLink,
      String baseFilename,
      IVariables variables) {
    String markdown = note != null ? note.getNote() : null;
    if (Utils.isEmpty(markdown)) {
      return new LayoutResult(20, 20, List.of());
    }

    NotePadType type = note.getNoteType() != null ? note.getNoteType() : NotePadType.GENERAL;
    NotePadStyle.RgbColor textRgb = NotePadStyle.textColor(type);
    NotePadStyle.RgbColor linkRgb = NotePadStyle.linkColor(type);
    NotePadStyle.RgbColor codeBg = NotePadStyle.codeBackground(type);

    int fontSize = baseFontSize > 0 ? baseFontSize : DEFAULT_FONT_SIZE;
    String effectiveFont = ConstNvl(fontName, "Sans");

    List<Block> blocks = BlockBuilder.build(markdown);
    List<NoteLinkHit> links = new ArrayList<>();

    int wrapWidth = contentWidth > 0 ? contentWidth : Integer.MAX_VALUE / 4;
    int cursorY = originY;
    int maxWidth = 0;

    for (int bi = 0; bi < blocks.size(); bi++) {
      Block block = blocks.get(bi);
      if (bi > 0) {
        cursorY += BLOCK_GAP;
      }
      Point size =
          paintBlock(
              gc,
              block,
              originX,
              cursorY,
              wrapWidth,
              effectiveFont,
              fontSize,
              textRgb,
              linkRgb,
              codeBg,
              paint,
              note,
              hoverLink,
              links,
              baseFilename,
              variables);
      maxWidth = Math.max(maxWidth, size.x);
      cursorY += size.y;
    }

    int height = Math.max(cursorY - originY, 1);
    return new LayoutResult(Math.max(maxWidth, 1), height, List.copyOf(links));
  }

  private static Point paintBlock(
      IGc gc,
      Block block,
      int x,
      int y,
      int wrapWidth,
      String fontName,
      int baseFontSize,
      NotePadStyle.RgbColor textRgb,
      NotePadStyle.RgbColor linkRgb,
      NotePadStyle.RgbColor codeBg,
      boolean paint,
      NotePadMeta note,
      NoteLinkHit hoverLink,
      List<NoteLinkHit> links,
      String baseFilename,
      IVariables variables) {
    return switch (block) {
      case HeadingBlock heading ->
          paintRunsBlock(
              gc,
              heading.runs(),
              x,
              y,
              wrapWidth,
              fontName,
              headingFontSize(baseFontSize, heading.level()),
              baseFontSize,
              true,
              false,
              textRgb,
              linkRgb,
              paint,
              note,
              hoverLink,
              links);
      case ParagraphBlock paragraph ->
          paintRunsBlock(
              gc,
              paragraph.runs(),
              x,
              y,
              wrapWidth,
              fontName,
              baseFontSize,
              baseFontSize,
              false,
              false,
              textRgb,
              linkRgb,
              paint,
              note,
              hoverLink,
              links);
      case ListBlock list ->
          paintList(
              gc,
              list,
              x,
              y,
              wrapWidth,
              fontName,
              baseFontSize,
              textRgb,
              linkRgb,
              paint,
              note,
              hoverLink,
              links,
              null);
      case CodeBlock code ->
          paintCodeBlock(
              gc, code.text(), x, y, wrapWidth, fontName, baseFontSize, textRgb, codeBg, paint);
      case TableBlockData table ->
          paintTable(
              gc,
              table,
              x,
              y,
              wrapWidth,
              fontName,
              baseFontSize,
              textRgb,
              linkRgb,
              paint,
              note,
              hoverLink,
              links);
      case HrBlock ignored -> paintHr(gc, x, y, wrapWidth, textRgb, paint);
      case ImageBlock image ->
          paintImageBlock(
              gc,
              image,
              x,
              y,
              wrapWidth,
              fontName,
              baseFontSize,
              textRgb,
              linkRgb,
              paint,
              note,
              hoverLink,
              links,
              baseFilename,
              variables);
    };
  }

  private static Point paintImageBlock(
      IGc gc,
      ImageBlock image,
      int x,
      int y,
      int wrapWidth,
      String fontName,
      int baseFontSize,
      NotePadStyle.RgbColor textRgb,
      NotePadStyle.RgbColor linkRgb,
      boolean paint,
      NotePadMeta note,
      NoteLinkHit hoverLink,
      List<NoteLinkHit> links,
      String baseFilename,
      IVariables variables) {
    String alt = Utils.isEmpty(image.alt()) ? "image" : image.alt();
    String resolved = null;
    try {
      resolved = NoteImageSupport.resolvePath(variables, baseFilename, image.src());
    } catch (Exception ignored) {
      resolved = null;
    }
    NoteImageSupport.ImageInfo info =
        resolved != null
            ? NoteImageSupport.probe(resolved)
            : new NoteImageSupport.ImageInfo(null, 0, 0, false, false);
    if (!info.available()) {
      // Placeholder when missing, network URL, or unloadable
      String label = "[" + alt + "]";
      return paintRunsBlock(
          gc,
          List.of(new Run(label, false, false, false, image.src())),
          x,
          y,
          wrapWidth,
          fontName,
          baseFontSize,
          baseFontSize,
          false,
          false,
          textRgb,
          linkRgb,
          paint,
          note,
          hoverLink,
          links);
    }
    Point size =
        NoteImageSupport.fit(
            info.width(), info.height(), wrapWidth, NoteImageSupport.DEFAULT_MAX_HEIGHT);
    if (paint) {
      boolean ok = gc.drawFileImage(info.path(), x, y, size.x, size.y);
      if (!ok) {
        String label = "[" + alt + "]";
        return paintRunsBlock(
            gc,
            List.of(new Run(label, false, false, false, image.src())),
            x,
            y,
            wrapWidth,
            fontName,
            baseFontSize,
            baseFontSize,
            false,
            false,
            textRgb,
            linkRgb,
            true,
            note,
            hoverLink,
            links);
      }
    }
    return size;
  }

  private static Point paintList(
      IGc gc,
      ListBlock list,
      int x,
      int y,
      int wrapWidth,
      String fontName,
      int baseFontSize,
      NotePadStyle.RgbColor textRgb,
      NotePadStyle.RgbColor linkRgb,
      boolean paint,
      NotePadMeta note,
      NoteLinkHit hoverLink,
      List<NoteLinkHit> links,
      List<PositionedLink> positionedLinks) {
    int cursorY = y;
    int maxW = 0;
    int index = list.startNumber();
    for (ListItemData item : list.items()) {
      String prefix =
          item.taskChecked() != null
              ? (Boolean.TRUE.equals(item.taskChecked()) ? "[x] " : "[ ] ")
              : (list.ordered() ? (index + ". ") : "• ");
      List<Run> runs = new ArrayList<>();
      runs.add(new Run(prefix, false, false, false, null));
      runs.addAll(item.runs());
      Point size;
      if (positionedLinks != null) {
        size =
            paintRunsPositioned(
                gc,
                runs,
                x + LIST_INDENT,
                cursorY,
                Math.max(1, wrapWidth - LIST_INDENT),
                fontName,
                baseFontSize,
                baseFontSize,
                false,
                false,
                textRgb,
                linkRgb,
                paint,
                note,
                hoverLink,
                links,
                positionedLinks);
      } else {
        size =
            paintRunsBlock(
                gc,
                runs,
                x + LIST_INDENT,
                cursorY,
                Math.max(1, wrapWidth - LIST_INDENT),
                fontName,
                baseFontSize,
                baseFontSize,
                false,
                false,
                textRgb,
                linkRgb,
                paint,
                note,
                hoverLink,
                links);
      }
      maxW = Math.max(maxW, LIST_INDENT + size.x);
      cursorY += size.y + 2;
      index++;
    }
    return new Point(maxW, Math.max(cursorY - y, 1));
  }

  private static Point paintCodeBlock(
      IGc gc,
      String text,
      int x,
      int y,
      int wrapWidth,
      String fontName,
      int baseFontSize,
      NotePadStyle.RgbColor textRgb,
      NotePadStyle.RgbColor codeBg,
      boolean paint) {
    String content = text == null ? "" : text;
    if (content.endsWith("\n")) {
      content = content.substring(0, content.length() - 1);
    }
    String[] lines = content.isEmpty() ? new String[] {""} : content.split("\n", -1);
    applyFont(gc, fontName, baseFontSize, false, false, baseFontSize);
    int lineHeight = Math.max(gc.textExtent("Ay").y, 1);
    int maxLineW = 0;
    for (String line : lines) {
      maxLineW = Math.max(maxLineW, gc.textExtent(line).x);
    }
    int pad = 4;
    int boxW = Math.min(wrapWidth, maxLineW + 2 * pad);
    if (wrapWidth < Integer.MAX_VALUE / 4) {
      boxW = wrapWidth;
    }
    int boxH = lines.length * lineHeight + 2 * pad;
    if (paint) {
      gc.setBackground(codeBg.red(), codeBg.green(), codeBg.blue());
      gc.fillRectangle(x, y, boxW, boxH);
      gc.setForeground(textRgb.red(), textRgb.green(), textRgb.blue());
      applyFont(gc, fontName, baseFontSize, false, false, baseFontSize);
      for (int i = 0; i < lines.length; i++) {
        gc.drawText(lines[i], x + pad, y + pad + i * lineHeight, true);
      }
    }
    return new Point(boxW, boxH);
  }

  private static Point paintHr(
      IGc gc, int x, int y, int wrapWidth, NotePadStyle.RgbColor textRgb, boolean paint) {
    int w = wrapWidth < Integer.MAX_VALUE / 4 ? wrapWidth : 120;
    if (paint) {
      gc.setForeground(textRgb.red(), textRgb.green(), textRgb.blue());
      gc.setLineWidth(1);
      gc.drawLine(x, y + 4, x + w, y + 4);
    }
    return new Point(w, 10);
  }

  private static Point paintTable(
      IGc gc,
      TableBlockData table,
      int x,
      int y,
      int wrapWidth,
      String fontName,
      int baseFontSize,
      NotePadStyle.RgbColor textRgb,
      NotePadStyle.RgbColor linkRgb,
      boolean paint,
      NotePadMeta note,
      NoteLinkHit hoverLink,
      List<NoteLinkHit> links) {
    if (table.rows().isEmpty()) {
      return new Point(0, 0);
    }
    int cols = 0;
    for (TableRowData row : table.rows()) {
      cols = Math.max(cols, row.cells().size());
    }
    if (cols == 0) {
      return new Point(0, 0);
    }

    applyFont(gc, fontName, baseFontSize, false, false, baseFontSize);
    int lineHeight = Math.max(gc.textExtent("Ay").y, 1);
    int[] colWidths = new int[cols];
    for (TableRowData row : table.rows()) {
      for (int c = 0; c < row.cells().size(); c++) {
        applyFont(gc, fontName, baseFontSize, row.header(), false, baseFontSize);
        int cellW =
            measureRunsWidth(
                gc, row.cells().get(c), fontName, baseFontSize, baseFontSize, row.header());
        colWidths[c] = Math.max(colWidths[c], cellW + 2 * CELL_PAD);
      }
    }

    int totalW = 0;
    for (int colWidth : colWidths) {
      totalW += colWidth;
    }
    if (wrapWidth < Integer.MAX_VALUE / 4 && totalW > wrapWidth && totalW > 0) {
      double scale = (double) wrapWidth / totalW;
      totalW = 0;
      for (int c = 0; c < cols; c++) {
        colWidths[c] = Math.max(20, (int) Math.floor(colWidths[c] * scale));
        totalW += colWidths[c];
      }
    }

    int rowH = lineHeight + 2 * CELL_PAD;
    int height = table.rows().size() * rowH;
    if (paint) {
      gc.setForeground(textRgb.red(), textRgb.green(), textRgb.blue());
      gc.setLineWidth(1);
      gc.drawRectangle(x, y, totalW, height);
      int cy = y;
      for (int r = 0; r < table.rows().size(); r++) {
        TableRowData row = table.rows().get(r);
        int cx = x;
        for (int c = 0; c < cols; c++) {
          if (c < row.cells().size()) {
            List<Run> cellRuns = row.cells().get(c);
            paintRunsBlock(
                gc,
                cellRuns,
                cx + CELL_PAD,
                cy + CELL_PAD,
                Math.max(1, colWidths[c] - 2 * CELL_PAD),
                fontName,
                baseFontSize,
                baseFontSize,
                row.header(),
                false,
                textRgb,
                linkRgb,
                true,
                note,
                hoverLink,
                links);
          }
          if (c > 0) {
            gc.drawLine(cx, cy, cx, cy + rowH);
          }
          cx += colWidths[c];
        }
        if (r > 0) {
          gc.drawLine(x, cy, x + totalW, cy);
        }
        cy += rowH;
        // Emphasize header boundary (GFM table header rule)
        if (row.header() && r == 0 && table.rows().size() > 1) {
          gc.setLineWidth(2);
          gc.drawLine(x, cy, x + totalW, cy);
          gc.setLineWidth(1);
        }
      }
    }
    return new Point(totalW, height);
  }

  private static Point paintRunsBlock(
      IGc gc,
      List<Run> runs,
      int x,
      int y,
      int wrapWidth,
      String fontName,
      int fontSize,
      int baseFontSize,
      boolean boldAll,
      boolean italicAll,
      NotePadStyle.RgbColor textRgb,
      NotePadStyle.RgbColor linkRgb,
      boolean paint,
      NotePadMeta note,
      NoteLinkHit hoverLink,
      List<NoteLinkHit> links) {
    List<Line> lines =
        wrapRuns(gc, runs, wrapWidth, fontName, fontSize, baseFontSize, boldAll, italicAll);
    int cursorY = y;
    int maxW = 0;
    for (Line line : lines) {
      int cursorX = x;
      int lineH = 0;
      for (RunSegment seg : line.segments()) {
        boolean bold = boldAll || seg.run().bold();
        boolean italic = italicAll || seg.run().italic();
        applyFont(gc, fontName, fontSize, bold, italic, baseFontSize);
        Point extent = gc.textExtent(seg.text());
        lineH = Math.max(lineH, extent.y);
        boolean isLink = !Utils.isEmpty(seg.run().linkTarget());
        if (paint) {
          NotePadStyle.RgbColor color = isLink ? linkRgb : textRgb;
          gc.setForeground(color.red(), color.green(), color.blue());
          gc.drawText(seg.text(), cursorX, cursorY, true);
          if (isLink) {
            boolean hover = isHovered(note, seg.run(), hoverLink);
            gc.setLineWidth(hover ? 2 : 1);
            gc.drawLine(
                cursorX, cursorY + extent.y - 1, cursorX + extent.x, cursorY + extent.y - 1);
            gc.setLineWidth(1);
            links.add(new NoteLinkHit(note, seg.text(), seg.run().linkTarget()));
            // bounds are stored by the painter via AreaOwners using segment screen position;
            // we attach absolute coords by wrapping in a positioned hit list below
          }
        } else if (isLink) {
          links.add(new NoteLinkHit(note, seg.text(), seg.run().linkTarget()));
        }
        cursorX += extent.x;
      }
      maxW = Math.max(maxW, cursorX - x);
      cursorY += Math.max(lineH, 1);
    }
    return new Point(maxW, Math.max(cursorY - y, 1));
  }

  /** Paint and collect link hits with absolute coordinates for AreaOwners. */
  public static LayoutResult paintWithLinkBounds(
      IGc gc,
      NotePadMeta note,
      String fontName,
      int baseFontSize,
      int originX,
      int originY,
      int contentWidth,
      NoteLinkHit hoverLink,
      List<PositionedLink> positionedLinks,
      String baseFilename,
      IVariables variables) {
    String markdown = note != null ? note.getNote() : null;
    if (Utils.isEmpty(markdown)) {
      return new LayoutResult(20, 20, List.of());
    }

    NotePadType type = note.getNoteType() != null ? note.getNoteType() : NotePadType.GENERAL;
    NotePadStyle.RgbColor textRgb = NotePadStyle.textColor(type);
    NotePadStyle.RgbColor linkRgb = NotePadStyle.linkColor(type);
    NotePadStyle.RgbColor codeBg = NotePadStyle.codeBackground(type);

    int fontSize = baseFontSize > 0 ? baseFontSize : DEFAULT_FONT_SIZE;
    String effectiveFont = ConstNvl(fontName, "Sans");
    List<Block> blocks = BlockBuilder.build(markdown);
    int wrapWidth = contentWidth > 0 ? contentWidth : Integer.MAX_VALUE / 4;
    int cursorY = originY;
    int maxWidth = 0;
    List<NoteLinkHit> links = new ArrayList<>();

    for (int bi = 0; bi < blocks.size(); bi++) {
      Block block = blocks.get(bi);
      if (bi > 0) {
        cursorY += BLOCK_GAP;
      }
      Point size =
          paintBlockPositioned(
              gc,
              block,
              originX,
              cursorY,
              wrapWidth,
              effectiveFont,
              fontSize,
              textRgb,
              linkRgb,
              codeBg,
              true,
              note,
              hoverLink,
              links,
              positionedLinks,
              baseFilename,
              variables);
      maxWidth = Math.max(maxWidth, size.x);
      cursorY += size.y;
    }
    return new LayoutResult(
        Math.max(maxWidth, 1), Math.max(cursorY - originY, 1), List.copyOf(links));
  }

  public record PositionedLink(NoteLinkHit hit, int x, int y, int width, int height) {}

  private static Point paintBlockPositioned(
      IGc gc,
      Block block,
      int x,
      int y,
      int wrapWidth,
      String fontName,
      int baseFontSize,
      NotePadStyle.RgbColor textRgb,
      NotePadStyle.RgbColor linkRgb,
      NotePadStyle.RgbColor codeBg,
      boolean paint,
      NotePadMeta note,
      NoteLinkHit hoverLink,
      List<NoteLinkHit> links,
      List<PositionedLink> positionedLinks,
      String baseFilename,
      IVariables variables) {
    if (block instanceof ListBlock list) {
      return paintList(
          gc,
          list,
          x,
          y,
          wrapWidth,
          fontName,
          baseFontSize,
          textRgb,
          linkRgb,
          paint,
          note,
          hoverLink,
          links,
          positionedLinks);
    }
    if (block instanceof ImageBlock image) {
      return paintImageBlock(
          gc,
          image,
          x,
          y,
          wrapWidth,
          fontName,
          baseFontSize,
          textRgb,
          linkRgb,
          paint,
          note,
          hoverLink,
          links,
          baseFilename,
          variables);
    }
    // Reuse non-positioned for non-run blocks; for run-based blocks collect positions.
    if (block instanceof CodeBlock || block instanceof HrBlock || block instanceof TableBlockData) {
      return paintBlock(
          gc,
          block,
          x,
          y,
          wrapWidth,
          fontName,
          baseFontSize,
          textRgb,
          linkRgb,
          codeBg,
          paint,
          note,
          hoverLink,
          links,
          baseFilename,
          variables);
    }
    List<Run> runs =
        switch (block) {
          case HeadingBlock h -> h.runs();
          case ParagraphBlock p -> p.runs();
          default -> List.of();
        };
    boolean boldAll = block instanceof HeadingBlock;
    int fontSize =
        block instanceof HeadingBlock h ? headingFontSize(baseFontSize, h.level()) : baseFontSize;
    return paintRunsPositioned(
        gc,
        runs,
        x,
        y,
        wrapWidth,
        fontName,
        fontSize,
        baseFontSize,
        boldAll,
        false,
        textRgb,
        linkRgb,
        paint,
        note,
        hoverLink,
        links,
        positionedLinks);
  }

  private static Point paintRunsPositioned(
      IGc gc,
      List<Run> runs,
      int x,
      int y,
      int wrapWidth,
      String fontName,
      int fontSize,
      int baseFontSize,
      boolean boldAll,
      boolean italicAll,
      NotePadStyle.RgbColor textRgb,
      NotePadStyle.RgbColor linkRgb,
      boolean paint,
      NotePadMeta note,
      NoteLinkHit hoverLink,
      List<NoteLinkHit> links,
      List<PositionedLink> positionedLinks) {
    List<Line> lines =
        wrapRuns(gc, runs, wrapWidth, fontName, fontSize, baseFontSize, boldAll, italicAll);
    int cursorY = y;
    int maxW = 0;
    for (Line line : lines) {
      int cursorX = x;
      int lineH = 0;
      for (RunSegment seg : line.segments()) {
        boolean bold = boldAll || seg.run().bold();
        boolean italic = italicAll || seg.run().italic();
        applyFont(gc, fontName, fontSize, bold, italic, baseFontSize);
        Point extent = gc.textExtent(seg.text());
        lineH = Math.max(lineH, extent.y);
        boolean isLink = !Utils.isEmpty(seg.run().linkTarget());
        if (paint) {
          NotePadStyle.RgbColor color = isLink ? linkRgb : textRgb;
          gc.setForeground(color.red(), color.green(), color.blue());
          gc.drawText(seg.text(), cursorX, cursorY, true);
          if (isLink) {
            boolean hover = isHovered(note, seg.run(), hoverLink);
            gc.setLineWidth(hover ? 2 : 1);
            gc.drawLine(
                cursorX, cursorY + extent.y - 1, cursorX + extent.x, cursorY + extent.y - 1);
            gc.setLineWidth(1);
            NoteLinkHit hit = new NoteLinkHit(note, seg.text(), seg.run().linkTarget());
            links.add(hit);
            positionedLinks.add(new PositionedLink(hit, cursorX, cursorY, extent.x, extent.y));
          }
        }
        cursorX += extent.x;
      }
      maxW = Math.max(maxW, cursorX - x);
      cursorY += Math.max(lineH, 1);
    }
    return new Point(maxW, Math.max(cursorY - y, 1));
  }

  private static boolean isHovered(NotePadMeta note, Run run, NoteLinkHit hover) {
    if (hover == null || note == null || run == null || Utils.isEmpty(run.linkTarget())) {
      return false;
    }
    return hover.note() == note && Objects.equals(hover.target(), run.linkTarget());
  }

  private static List<Line> wrapRuns(
      IGc gc,
      List<Run> runs,
      int wrapWidth,
      String fontName,
      int fontSize,
      int baseFontSize,
      boolean boldAll,
      boolean italicAll) {
    List<Line> lines = new ArrayList<>();
    List<RunSegment> current = new ArrayList<>();
    int lineWidth = 0;

    for (Run run : runs) {
      String text = run.text() != null ? run.text() : "";
      // Split on explicit newlines inside runs
      String[] parts = text.split("\n", -1);
      for (int pi = 0; pi < parts.length; pi++) {
        if (pi > 0) {
          lines.add(new Line(List.copyOf(current)));
          current.clear();
          lineWidth = 0;
        }
        String part = parts[pi];
        if (part.isEmpty()) {
          continue;
        }
        boolean bold = boldAll || run.bold();
        boolean italic = italicAll || run.italic();
        applyFont(gc, fontName, fontSize, bold, italic, baseFontSize);

        int remaining = part.length();
        int offset = 0;
        while (offset < remaining) {
          String rest = part.substring(offset);
          Point full = gc.textExtent(rest);
          if (lineWidth + full.x <= wrapWidth || current.isEmpty()) {
            // may still exceed if single token longer than wrap
            if (full.x > wrapWidth && current.isEmpty()) {
              // force-fit by characters
              int fit = fitCharacters(gc, rest, wrapWidth);
              String chunk = rest.substring(0, fit);
              current.add(new RunSegment(run, chunk));
              lines.add(new Line(List.copyOf(current)));
              current.clear();
              lineWidth = 0;
              offset += fit;
            } else {
              current.add(new RunSegment(run, rest));
              lineWidth += full.x;
              offset = remaining;
            }
          } else {
            // wrap before this rest
            lines.add(new Line(List.copyOf(current)));
            current.clear();
            lineWidth = 0;
            // retry same offset on new line
          }
        }
      }
    }
    if (!current.isEmpty() || lines.isEmpty()) {
      lines.add(new Line(List.copyOf(current)));
    }
    return lines;
  }

  private static int fitCharacters(IGc gc, String text, int maxWidth) {
    if (text.isEmpty()) {
      return 0;
    }
    int lo = 1;
    int hi = text.length();
    int best = 1;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      if (gc.textExtent(text.substring(0, mid)).x <= maxWidth) {
        best = mid;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return Math.max(1, best);
  }

  private static int measureRunsWidth(
      IGc gc, List<Run> runs, String fontName, int fontSize, int baseFontSize, boolean boldAll) {
    int w = 0;
    for (Run run : runs) {
      applyFont(gc, fontName, fontSize, boldAll || run.bold(), run.italic(), baseFontSize);
      w += gc.textExtent(run.text() != null ? run.text() : "").x;
    }
    return w;
  }

  private static int headingFontSize(int base, int level) {
    // Proportional steps so small HiDPI graph bases stay balanced
    int b = Math.max(base, 1);
    return switch (level) {
      case 1 -> Math.max(b + 1, (int) Math.round(b * 1.45));
      case 2 -> Math.max(b + 1, (int) Math.round(b * 1.3));
      case 3 -> Math.max(b + 1, (int) Math.round(b * 1.15));
      default -> b + 1;
    };
  }

  /**
   * Apply a font for a text run. When the size matches the graph base size and italic is off, use
   * {@link IGc.EFont#GRAPH} / {@link IGc.EFont#GRAPH_BOLD} so Markdown body text is identical to
   * transform/action names (same ManagedFont + canvas magnification). Headings and italics use an
   * explicit point size derived from that base.
   *
   * <p>Important: do not clamp the size before comparing to {@code baseSize}. On HiDPI the graph
   * font is often only 4–5pt after zoom compensation; clamping to 6 broke the GRAPH path and built
   * a larger custom font (~50% too big).
   */
  private static void applyFont(
      IGc gc, String fontName, int fontSize, boolean bold, boolean italic, int baseSize) {
    if (fontSize == baseSize && !italic) {
      gc.setFont(bold ? IGc.EFont.GRAPH_BOLD : IGc.EFont.GRAPH);
      return;
    }
    // Headings / italics: explicit size, still relative to the graph base
    String name = ConstNvl(fontName, "Sans");
    gc.setFont(name, Math.max(fontSize, 1), bold, italic);
  }

  private static String ConstNvl(String value, String def) {
    return value == null || value.isEmpty() ? def : value;
  }

  // --- intermediate model ---

  private record Run(String text, boolean bold, boolean italic, boolean code, String linkTarget) {}

  private record RunSegment(Run run, String text) {}

  private record Line(List<RunSegment> segments) {}

  private sealed interface Block
      permits HeadingBlock,
          ParagraphBlock,
          ListBlock,
          CodeBlock,
          TableBlockData,
          HrBlock,
          ImageBlock {}

  private record HeadingBlock(int level, List<Run> runs) implements Block {}

  private record ParagraphBlock(List<Run> runs) implements Block {}

  private record ListItemData(List<Run> runs, Boolean taskChecked) {}

  private record ListBlock(boolean ordered, int startNumber, List<ListItemData> items)
      implements Block {}

  private record CodeBlock(String text) implements Block {}

  private record TableRowData(boolean header, List<List<Run>> cells) {}

  private record TableBlockData(List<TableRowData> rows) implements Block {}

  private record HrBlock() implements Block {}

  private record ImageBlock(String alt, String src) implements Block {}

  private static final class BlockBuilder extends AbstractVisitor {
    private final List<Block> blocks = new ArrayList<>();
    private final List<Run> runBuffer = new ArrayList<>();
    private boolean bold;
    private boolean italic;
    private String linkTarget;

    static List<Block> build(String markdown) {
      BlockBuilder builder = new BlockBuilder();
      CommonMarkConfig.parse(markdown).accept(builder);
      return builder.blocks;
    }

    @Override
    public void visit(Document document) {
      visitChildren(document);
    }

    @Override
    public void visit(Heading heading) {
      runBuffer.clear();
      visitChildren(heading);
      blocks.add(new HeadingBlock(heading.getLevel(), List.copyOf(runBuffer)));
      runBuffer.clear();
    }

    @Override
    public void visit(Paragraph paragraph) {
      if (paragraph.getParent() instanceof ListItem) {
        visitChildren(paragraph);
        return;
      }
      runBuffer.clear();
      visitChildren(paragraph);
      if (!runBuffer.isEmpty()) {
        blocks.add(new ParagraphBlock(List.copyOf(runBuffer)));
      }
      runBuffer.clear();
    }

    @Override
    public void visit(BulletList bulletList) {
      blocks.add(buildList(bulletList, false, 1));
    }

    @Override
    public void visit(OrderedList orderedList) {
      blocks.add(buildList(orderedList, true, orderedList.getStartNumber()));
    }

    private ListBlock buildList(Node listNode, boolean ordered, int start) {
      List<ListItemData> items = new ArrayList<>();
      for (Node node = listNode.getFirstChild(); node != null; node = node.getNext()) {
        if (node instanceof ListItem item) {
          runBuffer.clear();
          Boolean task = null;
          Node child = item.getFirstChild();
          if (child instanceof TaskListItemMarker marker) {
            task = marker.isChecked();
            child = child.getNext();
          }
          for (Node n = child; n != null; n = n.getNext()) {
            n.accept(this);
          }
          // visitChildren for list item paragraphs fills runBuffer via special-case
          items.add(new ListItemData(List.copyOf(runBuffer), task));
          runBuffer.clear();
        }
      }
      return new ListBlock(ordered, start, items);
    }

    @Override
    public void visit(FencedCodeBlock fencedCodeBlock) {
      blocks.add(new CodeBlock(fencedCodeBlock.getLiteral()));
    }

    @Override
    public void visit(IndentedCodeBlock indentedCodeBlock) {
      blocks.add(new CodeBlock(indentedCodeBlock.getLiteral()));
    }

    @Override
    public void visit(ThematicBreak thematicBreak) {
      blocks.add(new HrBlock());
    }

    @Override
    public void visit(CustomBlock customBlock) {
      if (customBlock instanceof TableBlock tableBlock) {
        blocks.add(buildTable(tableBlock));
        return;
      }
      visitChildren(customBlock);
    }

    private TableBlockData buildTable(TableBlock tableBlock) {
      List<TableRowData> rows = new ArrayList<>();
      boolean inHead = false;
      for (Node node = tableBlock.getFirstChild(); node != null; node = node.getNext()) {
        if (node instanceof TableHead) {
          inHead = true;
          for (Node rowNode = node.getFirstChild(); rowNode != null; rowNode = rowNode.getNext()) {
            if (rowNode instanceof TableRow row) {
              rows.add(extractRow(row, true));
            }
          }
          inHead = false;
        } else if (node instanceof TableRow row) {
          rows.add(extractRow(row, inHead));
        } else {
          // TableBody etc.
          for (Node rowNode = node.getFirstChild(); rowNode != null; rowNode = rowNode.getNext()) {
            if (rowNode instanceof TableRow row) {
              rows.add(extractRow(row, false));
            }
          }
        }
      }
      return new TableBlockData(rows);
    }

    private TableRowData extractRow(TableRow row, boolean header) {
      List<List<Run>> cells = new ArrayList<>();
      for (Node node = row.getFirstChild(); node != null; node = node.getNext()) {
        if (node instanceof TableCell cell) {
          runBuffer.clear();
          visitChildren(cell);
          cells.add(List.copyOf(runBuffer));
          runBuffer.clear();
        }
      }
      return new TableRowData(header, cells);
    }

    @Override
    public void visit(CustomNode customNode) {
      visitChildren(customNode);
    }

    @Override
    public void visit(StrongEmphasis strongEmphasis) {
      boolean prev = bold;
      bold = true;
      visitChildren(strongEmphasis);
      bold = prev;
    }

    @Override
    public void visit(Emphasis emphasis) {
      boolean prev = italic;
      italic = true;
      visitChildren(emphasis);
      italic = prev;
    }

    @Override
    public void visit(Code code) {
      runBuffer.add(new Run(code.getLiteral(), bold, italic, true, linkTarget));
    }

    @Override
    public void visit(Link link) {
      String prev = linkTarget;
      linkTarget = link.getDestination();
      visitChildren(link);
      linkTarget = prev;
    }

    @Override
    public void visit(Image image) {
      String alt = extractPlain(image);
      if (alt.isEmpty()) {
        alt = "image";
      }
      // Flush any pending paragraph text, then place image as its own block for layout.
      if (!runBuffer.isEmpty()) {
        blocks.add(new ParagraphBlock(List.copyOf(runBuffer)));
        runBuffer.clear();
      }
      blocks.add(new ImageBlock(alt, image.getDestination()));
    }

    @Override
    public void visit(Text text) {
      runBuffer.add(new Run(text.getLiteral(), bold, italic, false, linkTarget));
    }

    @Override
    public void visit(SoftLineBreak softLineBreak) {
      runBuffer.add(new Run(" ", bold, italic, false, linkTarget));
    }

    @Override
    public void visit(HardLineBreak hardLineBreak) {
      runBuffer.add(new Run("\n", bold, italic, false, linkTarget));
    }

    private static String extractPlain(Node node) {
      StringBuilder sb = new StringBuilder();
      node.accept(
          new AbstractVisitor() {
            @Override
            public void visit(Text text) {
              sb.append(text.getLiteral());
            }
          });
      return sb.toString();
    }
  }
}
