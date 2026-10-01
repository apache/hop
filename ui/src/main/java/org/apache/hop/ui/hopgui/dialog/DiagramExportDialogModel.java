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

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.diagram.DiagramExportFormat;
import org.apache.hop.core.diagram.DiagramExportOptions;
import org.apache.hop.core.diagram.DiagramExportService;
import org.apache.hop.core.diagram.IDiagramExporter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/** Model backing the diagram export dialog using annotated GuiWidgetElement fields. */
@GuiPlugin(id = "diagram-export-dialog-model", description = "Diagram export options")
@Getter
@Setter
public class DiagramExportDialogModel {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "DiagramExportDialogModel-Widgets";

  public static final String WIDGET_FORMAT = "diagram-export-format";
  public static final String WIDGET_FILENAME = "diagram-export-filename";
  public static final String WIDGET_MAGNIFICATION = "diagram-export-magnification";
  public static final String WIDGET_INCLUDE_NOTES = "diagram-export-include-notes";

  private final Object subject;
  private final List<IDiagramExporter<?>> supportedExporters;

  @GuiWidgetElement(
      id = WIDGET_FORMAT,
      order = "0100",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.COMBO,
      variables = false,
      comboValuesMethod = "getFormatNames",
      label = "i18n::DiagramExportDialog.Format.Label",
      toolTip = "i18n::DiagramExportDialog.Format.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::DiagramExportDialog.Group.ExportSettings")
  @HopMetadataProperty
  private String format;

  @GuiWidgetElement(
      id = WIDGET_FILENAME,
      order = "0200",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FILENAME,
      label = "i18n::DiagramExportDialog.Filename.Label",
      toolTip = "i18n::DiagramExportDialog.Filename.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::DiagramExportDialog.Group.ExportSettings")
  @HopMetadataProperty
  private String filename;

  @GuiWidgetElement(
      id = WIDGET_MAGNIFICATION,
      order = "0300",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::DiagramExportDialog.Magnification.Label",
      toolTip = "i18n::DiagramExportDialog.Magnification.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::DiagramExportDialog.Group.ExportSettings")
  @HopMetadataProperty
  private String magnification = "1.0";

  @GuiWidgetElement(
      id = WIDGET_INCLUDE_NOTES,
      order = "0400",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::DiagramExportDialog.IncludeNotes.Label",
      toolTip = "i18n::DiagramExportDialog.IncludeNotes.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = "i18n::DiagramExportDialog.Group.ExportSettings")
  @HopMetadataProperty
  private boolean includeNotes = true;

  public DiagramExportDialogModel() {
    this(null, "");
  }

  public DiagramExportDialogModel(Object subject, String defaultFilename) {
    this.subject = subject;
    this.supportedExporters =
        subject != null
            ? DiagramExportService.getInstance().findExportersForSubject(subject)
            : DiagramExportService.getInstance().getAllExporters();

    if (!supportedExporters.isEmpty()) {
      this.format = formatLabel(supportedExporters.get(0));
    } else {
      this.format = DiagramExportFormat.SVG.getName();
    }
    this.filename = defaultFilename;
    updateExtensionForSelectedFormat();
  }

  public List<String> getFormatNames(ILogChannel log, IHopMetadataProvider metadataProvider) {
    List<String> names = new ArrayList<>();
    for (IDiagramExporter<?> exporter : supportedExporters) {
      String fmtName = formatLabel(exporter);
      if (StringUtils.isNotBlank(fmtName) && !names.contains(fmtName)) {
        names.add(fmtName);
      }
    }
    if (names.isEmpty()) {
      names.add(DiagramExportFormat.SVG.getName());
      names.add(DiagramExportFormat.MERMAID.getName());
    }
    return names;
  }

  public IDiagramExporter<?> getSelectedExporter() {
    if (StringUtils.isNotBlank(format)) {
      for (IDiagramExporter<?> exporter : supportedExporters) {
        if (matchesExporter(exporter, format)) {
          return exporter;
        }
      }
    }
    return !supportedExporters.isEmpty() ? supportedExporters.get(0) : null;
  }

  public void updateExtensionForSelectedFormat() {
    if (StringUtils.isBlank(filename)) {
      return;
    }
    IDiagramExporter<?> exporter = getSelectedExporter();
    if (exporter == null || StringUtils.isBlank(exporter.getFileExtension())) {
      return;
    }
    String targetExt = "." + exporter.getFileExtension();
    int lastSep = Math.max(filename.lastIndexOf('/'), filename.lastIndexOf('\\'));
    int lastDot = filename.lastIndexOf('.');
    if (lastDot > lastSep) {
      filename = filename.substring(0, lastDot) + targetExt;
    } else {
      filename = filename + targetExt;
    }
  }

  public DiagramExportOptions toOptions() {
    DiagramExportOptions options = new DiagramExportOptions();
    options.setTargetFilename(filename);
    IDiagramExporter<?> exporter = getSelectedExporter();
    if (exporter != null && exporter.getFormat() != null) {
      options.setFormat(exporter.getFormat().getId());
    } else {
      options.setFormat(format);
    }
    try {
      options.setMagnification(Float.parseFloat(magnification));
    } catch (Exception e) {
      options.setMagnification(1.0f);
    }
    options.setIncludeNotes(includeNotes);
    return options;
  }

  static String formatLabel(IDiagramExporter<?> exporter) {
    if (exporter == null) {
      return "";
    }
    if (exporter.getFormat() != null && StringUtils.isNotBlank(exporter.getFormat().getName())) {
      return exporter.getFormat().getName();
    }
    return StringUtils.isNotBlank(exporter.getName()) ? exporter.getName() : exporter.getId();
  }

  static boolean matchesExporter(IDiagramExporter<?> exporter, String selected) {
    if (exporter == null || StringUtils.isBlank(selected)) {
      return false;
    }
    if (selected.equalsIgnoreCase(formatLabel(exporter))) {
      return true;
    }
    if (selected.equalsIgnoreCase(exporter.getId())
        || selected.equalsIgnoreCase(exporter.getName())) {
      return true;
    }
    if (exporter.getFormat() != null) {
      return selected.equalsIgnoreCase(exporter.getFormat().getId())
          || selected.equalsIgnoreCase(exporter.getFormat().getName())
          || selected.equalsIgnoreCase(exporter.getFormat().getDefaultExtension());
    }
    return selected.equalsIgnoreCase(exporter.getFileExtension());
  }
}
