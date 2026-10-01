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

package org.apache.hop.ui.core.dialog;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.SvgGc;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.svg.HopSvgGraphics2D;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.ui.core.PropsUi;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/** Renders ContextDialog category headers and action tiles into vector SVG XML. */
public final class ContextDialogSvgRenderer {

  private ContextDialogSvgRenderer() {}

  public static ContextDialogSvgRenderResult render(
      ContextDialog dialog, int areaWidth, int areaHeight) throws HopException {
    boolean darkMode = false;
    Map<String, String> contrastingColors = null;
    String fontName = "Arial";
    int fontHeight = 11;

    try {
      PropsUi props = PropsUi.getInstance();
      darkMode = props.isDarkMode();
      contrastingColors = darkMode ? props.getContrastingColorStrings() : null;
      if (props.getDefaultFont() != null && props.getDefaultFont().getName() != null) {
        fontName = props.getDefaultFont().getName();
      }
      fontHeight = (int) Math.round(props.getZoomFactor() * 11);
      if (fontHeight <= 0) {
        fontHeight = 11;
      }
    } catch (Throwable ignored) {
      // Fallback in headless / unit-test environments without PropsUi / Display
    }
    return render(dialog, areaWidth, areaHeight, darkMode, contrastingColors, fontName, fontHeight);
  }

  public static ContextDialogSvgRenderResult render(
      ContextDialog dialog,
      int areaWidth,
      int areaHeight,
      boolean darkMode,
      Map<String, String> contrastingColors,
      String fontName,
      int fontHeight)
      throws HopException {
    HopSvgGraphics2D graphics2D = HopSvgGraphics2D.newDocument();
    int iconSize = dialog.getIconSize();
    int margin = dialog.getMargin();
    int xMargin = dialog.getXMargin();
    int yMargin = dialog.getYMargin();

    Point initialArea = new Point(Math.max(areaWidth, 100), Math.max(areaHeight, 100));
    SvgGc gc = new SvgGc(graphics2D, initialArea, iconSize, 0, 0, darkMode, contrastingColors);

    List<AreaOwner> areaOwners = new ArrayList<>();
    List<ContextDialog.CategoryAndOrder> categories = dialog.getCategories();
    List<ContextDialog.Item> filteredItems = dialog.getFilteredItems();
    boolean useCategories = dialog.isUseCategories();
    boolean useFixedWidth = dialog.isUseFixedWidth();

    int height = 0;
    int categoryNr = 0;
    int x = margin;
    int y = margin;

    while ((useCategories && categories != null && categoryNr < categories.size())
        || (!useCategories || categories == null || categories.isEmpty()) && (categoryNr == 0)) {

      ContextDialog.CategoryAndOrder categoryAndOrder;
      if (!useCategories || categories == null || categories.isEmpty()) {
        categoryAndOrder = null;
      } else {
        categoryAndOrder = categories.get(categoryNr);
      }

      List<ContextDialog.Item> itemsToPaint = findItemsForCategory(filteredItems, categoryAndOrder);

      if (!itemsToPaint.isEmpty()) {
        if (categoryAndOrder != null) {
          // Category header
          gc.setFont(fontName, fontHeight + 1, true, true);
          if (categoryAndOrder.isCollapsed()) {
            if (darkMode) {
              gc.setForeground(140, 140, 140);
            } else {
              gc.setForeground(128, 128, 128);
            }
          } else {
            if (darkMode) {
              gc.setForeground(240, 240, 240);
            } else {
              gc.setForeground(0, 0, 0);
            }
          }
          Point categoryExtent = gc.textExtent(categoryAndOrder.getCategory());
          gc.drawText(categoryAndOrder.getCategory(), x, y);
          areaOwners.add(
              new AreaOwner(
                  AreaOwner.AreaType.CUSTOM,
                  x,
                  y,
                  categoryExtent.x,
                  categoryExtent.y,
                  new DPoint(0, 0),
                  ContextDialog.OwnerType.CATEGORY,
                  categoryAndOrder));
          y += categoryExtent.y + yMargin;
          gc.setLineWidth(1);
          if (darkMode) {
            gc.setForeground(70, 70, 70);
          } else {
            gc.setForeground(215, 215, 215);
          }
          gc.drawLine(margin, y - yMargin, areaWidth - xMargin, y - yMargin);
        }

        if (darkMode) {
          gc.setForeground(232, 232, 232);
        } else {
          gc.setForeground(0, 0, 0);
        }
        gc.setFont(fontName, fontHeight, false, false);

        if (categoryAndOrder == null || !categoryAndOrder.isCollapsed()) {
          Map<GuiAction, ActionDetails> detailsMap = new HashMap<>();

          for (ContextDialog.Item item : itemsToPaint) {
            ActionDetails details = new ActionDetails();
            details.name = Const.NVL(item.getAction().getName(), item.getAction().getId());
            Point nameExtent = gc.textExtent(details.name);
            details.nameExtent = new org.eclipse.swt.graphics.Point(nameExtent.x, nameExtent.y);
            details.width = Math.max(nameExtent.x, iconSize);
            details.height = nameExtent.y + margin + iconSize;
            detailsMap.put(item.getAction(), details);
          }

          if (useFixedWidth) {
            int maxWidth = 0;
            for (ActionDetails details : detailsMap.values()) {
              maxWidth = Math.max(maxWidth, details.width);
            }
            for (ActionDetails details : detailsMap.values()) {
              details.width = maxWidth;
            }
          }

          for (ContextDialog.Item item : itemsToPaint) {
            ActionDetails details = detailsMap.get(item.getAction());
            int width = details.width;
            height = details.height;

            if (x + width + xMargin > areaWidth) {
              x = margin;
              y += height + yMargin;
            }

            if (item.isSelected()) {
              if (darkMode) {
                gc.setBackground(15, 136, 210);
              } else {
                gc.setBackground(201, 232, 251);
              }
              gc.setLineWidth(2);
              gc.fillRoundRectangle(
                  x - xMargin / 2,
                  y - yMargin / 2,
                  width + xMargin,
                  height + yMargin,
                  margin,
                  margin);
            }

            int imageMargin = (width - iconSize) / 2;
            if (StringUtils.isNotEmpty(item.getAction().getImage())) {
              ClassLoader cl = item.getAction().getClassLoader();
              if (cl == null) {
                cl = ClassLoader.getSystemClassLoader();
              }
              try {
                SvgFile svgFile = new SvgFile(item.getAction().getImage(), cl);
                gc.drawImage(svgFile, x + imageMargin, y, iconSize, iconSize, 1.0f, 0);
              } catch (Exception ignored) {
                // Ignore image draw failure
              }
            }

            gc.setFont(fontName, fontHeight, false, false);
            if (darkMode) {
              gc.setForeground(232, 232, 232);
            } else {
              gc.setForeground(0, 0, 0);
            }
            int textMargin = (width - details.nameExtent.x) / 2;
            gc.drawText(details.name, x + textMargin, y + iconSize + margin);

            AreaOwner areaOwner =
                new AreaOwner(
                    AreaOwner.AreaType.CUSTOM,
                    x,
                    y,
                    width,
                    height,
                    new DPoint(0, 0),
                    ContextDialog.OwnerType.ITEM,
                    item);
            areaOwners.add(areaOwner);
            item.setAreaOwner(areaOwner);

            x += width + xMargin;
            if (x > areaWidth) {
              x = margin;
              y += height + yMargin;
            }
          }

          x = margin;
          y += height + yMargin;
        } else {
          y -= yMargin;
        }
      }

      categoryNr++;
      if (!itemsToPaint.isEmpty()) {
        y += yMargin;
      }
    }

    int totalContentHeight = Math.max(areaHeight, y);

    try {
      Element root = (Element) graphics2D.getRoot();
      root.setAttribute("width", "100%");
      root.setAttribute("height", String.valueOf(totalContentHeight));
      root.setAttribute("viewBox", "0 0 " + areaWidth + " " + totalContentHeight);
      root.setAttribute("preserveAspectRatio", "none");

      // Remove the SvgGc constructor's initial background rectangle so the SVG remains transparent
      // and uniformly blends with the dialog's theme background across the entire scrollable
      // height.
      NodeList rects = root.getElementsByTagName("rect");
      for (int i = 0; i < rects.getLength(); i++) {
        Element rect = (Element) rects.item(i);
        if ("0".equals(rect.getAttribute("x"))
            && "0".equals(rect.getAttribute("y"))
            && !rect.hasAttribute("rx")) {
          rect.getParentNode().removeChild(rect);
          break;
        }
      }

      // Nobody reads this document: indentation is 15% of a payload that is sent on every
      // keystroke.
      Transformer transformer = XmlHandler.createSecureTransformerFactory().newTransformer();
      transformer.setOutputProperty(OutputKeys.INDENT, "no");
      StringWriter writer = new StringWriter();
      transformer.transform(new DOMSource(root), new StreamResult(writer));
      return new ContextDialogSvgRenderResult(writer.toString(), areaOwners, totalContentHeight);
    } catch (Exception e) {
      throw new HopException("Unable to serialize ContextDialog SVG", e);
    }
  }

  static List<ContextDialog.Item> findItemsForCategory(
      List<ContextDialog.Item> filteredItems, ContextDialog.CategoryAndOrder categoryAndOrder) {
    List<ContextDialog.Item> list = new ArrayList<>();
    if (filteredItems == null) {
      return list;
    }
    for (ContextDialog.Item filteredItem : filteredItems) {
      if (categoryAndOrder == null
          || categoryAndOrder
              .getCategory()
              .equalsIgnoreCase(filteredItem.getAction().getCategory())) {
        list.add(filteredItem);
      } else if (ContextDialog.CATEGORY_OTHER.equals(categoryAndOrder.getCategory())
          && StringUtils.isEmpty(filteredItem.getAction().getCategory())) {
        list.add(filteredItem);
      }
    }
    return list;
  }
}
