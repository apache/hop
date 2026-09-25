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

package org.apache.hop.ui.hopgui.perspective.explorer.file;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.parquet.explorer.ParquetColumnView;
import org.apache.hop.parquet.explorer.ParquetFileInspection;
import org.apache.hop.parquet.explorer.ParquetFileInspector;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.RowPreviewSupport;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.editor.IContentEditorWidget;
import org.apache.hop.ui.hopgui.ContentEditorFacade;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.base.BaseExplorerFileTypeHandler;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.TableItem;

/** Read-only explorer tab for a Parquet file: details, schema and a short data preview. */
public class ParquetExplorerFileTypeHandler extends BaseExplorerFileTypeHandler {

  private static final Class<?> PKG = ParquetExplorerFileTypeHandler.class;

  private Label note;
  private TableView detailsView;
  private IContentEditorWidget schemaEditor;
  private TableView columnsView;
  private Composite previewComposite;
  private Label previewLabel;
  private TableView previewView;

  public ParquetExplorerFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile explorerFile) {
    super(hopGui, perspective, explorerFile);
  }

  @Override
  public void renderFile(Composite composite) {
    PropsUi.setLook(composite);

    CTabFolder folder = new CTabFolder(composite, SWT.BORDER);
    PropsUi.setLook(folder, PropsUi.WIDGET_STYLE_TAB);
    folder.setLayoutData(new FormDataBuilder().fullSize().result());

    CTabItem detailsTab = new CTabItem(folder, SWT.NONE);
    detailsTab.setText(BaseMessages.getString(PKG, "ParquetExplorer.Tab.Details"));
    detailsTab.setControl(buildDetails(folder));

    CTabItem structureTab = new CTabItem(folder, SWT.NONE);
    structureTab.setText(BaseMessages.getString(PKG, "ParquetExplorer.Tab.Structure"));
    structureTab.setControl(buildStructure(folder));

    CTabItem previewTab = new CTabItem(folder, SWT.NONE);
    previewTab.setText(BaseMessages.getString(PKG, "ParquetExplorer.Tab.Preview"));
    previewTab.setControl(buildPreview(folder));

    folder.setSelection(0);

    String filename = explorerFile.getFilename();
    IVariables variables = getVariables();
    Thread reader = new Thread(() -> load(composite, filename, variables), "Parquet file preview");
    reader.setDaemon(true);
    reader.start();
  }

  private Composite buildDetails(Composite parent) {
    Composite details = new Composite(parent, SWT.NONE);
    details.setLayout(new FormLayout());
    PropsUi.setLook(details);

    note = new Label(details, SWT.WRAP);
    PropsUi.setLook(note);
    note.setText(BaseMessages.getString(PKG, "ParquetExplorer.Note"));
    note.setToolTipText(note.getText());
    // A wrapping label only keeps its wrapped height when the layout gives it one.
    int noteHeight = note.computeSize(SWT.DEFAULT, SWT.DEFAULT).y * 3;
    note.setLayoutData(new FormDataBuilder().top().fullWidth().height(noteHeight).result());

    detailsView =
        readOnlyTable(
            details,
            new ColumnInfo[] {
              column("ParquetExplorer.Details.Column.Property"),
              column("ParquetExplorer.Details.Column.Value")
            });
    detailsView.setLayoutData(
        new FormDataBuilder().top(note, PropsUi.getMargin()).bottom().fullWidth().result());
    return details;
  }

  private Composite buildStructure(Composite parent) {
    Composite structure = new Composite(parent, SWT.NONE);
    structure.setLayout(new FormLayout());
    PropsUi.setLook(structure);

    SashForm sash = new SashForm(structure, SWT.VERTICAL);
    PropsUi.setLook(sash);
    sash.setLayoutData(new FormDataBuilder().fullSize().result());

    Composite jsonParent = new Composite(sash, SWT.NONE);
    jsonParent.setLayout(new FormLayout());
    PropsUi.setLook(jsonParent);
    schemaEditor = ContentEditorFacade.createContentEditor(jsonParent, "json");
    schemaEditor.getControl().setLayoutData(new FormDataBuilder().fullSize().result());
    schemaEditor.setReadOnly(true);
    schemaEditor.setText(BaseMessages.getString(PKG, "ParquetExplorer.Reading"));

    columnsView =
        readOnlyTable(
            sash,
            new ColumnInfo[] {
              column("ParquetExplorer.Structure.Column.Name"),
              column("ParquetExplorer.Structure.Column.ParquetType"),
              column("ParquetExplorer.Structure.Column.HopType"),
              column("ParquetExplorer.Structure.Column.Length"),
              column("ParquetExplorer.Structure.Column.Precision")
            });
    sash.setWeights(1, 1);
    return structure;
  }

  private Composite buildPreview(Composite parent) {
    previewComposite = new Composite(parent, SWT.NONE);
    previewComposite.setLayout(new FormLayout());
    PropsUi.setLook(previewComposite);

    previewLabel = new Label(previewComposite, SWT.WRAP);
    PropsUi.setLook(previewLabel);
    previewLabel.setText(BaseMessages.getString(PKG, "ParquetExplorer.Reading"));
    previewLabel.setLayoutData(new FormDataBuilder().top().fullWidth().result());
    return previewComposite;
  }

  private TableView readOnlyTable(Composite parent, ColumnInfo[] columns) {
    TableView table =
        new TableView(
            getVariables(),
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            0,
            true,
            null,
            PropsUi.getInstance(),
            false,
            null,
            false,
            false);
    table.setReadonly(true);
    PropsUi.setLook(table);
    return table;
  }

  private static ColumnInfo column(String key) {
    return new ColumnInfo(
        BaseMessages.getString(PKG, key), ColumnInfo.COLUMN_TYPE_TEXT, false, true);
  }

  private void load(Composite composite, String filename, IVariables variables) {
    try {
      ParquetFileInspection inspection = ParquetFileInspector.inspect(filename, variables);
      async(
          composite,
          () -> {
            if (!composite.isDisposed()) {
              show(inspection);
            }
          });
    } catch (Exception e) {
      async(composite, () -> showError(composite, e));
    }
  }

  private void async(Composite composite, Runnable action) {
    if (hopGui == null || hopGui.getDisplay() == null || hopGui.getDisplay().isDisposed()) {
      return;
    }
    hopGui
        .getDisplay()
        .asyncExec(
            () -> {
              if (!composite.isDisposed()) {
                action.run();
              }
            });
  }

  private void show(ParquetFileInspection inspection) {
    if (note.isDisposed()) {
      return;
    }
    fillRows(
        detailsView,
        List.of(
            detail("ParquetExplorer.Details.Name", inspection.getFileName()),
            detail("ParquetExplorer.Details.Folder", inspection.getFolder()),
            detail("ParquetExplorer.Details.Size", grouped(inspection.getSizeBytes())),
            detail("ParquetExplorer.Details.Compression", inspection.getCompression()),
            detail("ParquetExplorer.Details.Version", inspection.getVersion()),
            detail("ParquetExplorer.Details.RowGroupSize", grouped(inspection.getRowGroupSize())),
            detail("ParquetExplorer.Details.DataPageSize", grouped(inspection.getDataPageSize())),
            detail(
                "ParquetExplorer.Details.DictionaryPageSize",
                grouped(inspection.getDictionaryPageSize())),
            detail("ParquetExplorer.Details.Rows", grouped(inspection.getRowCount())),
            detail(
                "ParquetExplorer.Details.RowGroups",
                Integer.toString(inspection.getRowGroupCount())),
            detail("ParquetExplorer.Details.CreatedBy", inspection.getCreatedBy())));
    detailsView.table.setToolTipText(note.getText());

    schemaEditor.setText(inspection.getSchemaJson());
    List<String[]> columnRows = new ArrayList<>();
    for (ParquetColumnView column : inspection.getColumns()) {
      columnRows.add(
          new String[] {
            column.getName(),
            column.getParquetType(),
            column.getHopType(),
            number(column.getLength()),
            number(column.getPrecision())
          });
    }
    fillRows(columnsView, columnRows);
    showPreview(inspection);
  }

  private void showPreview(ParquetFileInspection inspection) {
    if (previewLabel.isDisposed()) {
      return;
    }
    if (inspection.getPreviewError() != null) {
      previewLabel.setText(
          BaseMessages.getString(
              PKG, "ParquetExplorer.Preview.Failed", inspection.getPreviewError()));
      return;
    }
    int shown = inspection.getPreviewRows().size();
    if (shown >= ParquetFileInspector.PREVIEW_ROW_LIMIT) {
      previewLabel.setText(
          BaseMessages.getString(
              PKG,
              "ParquetExplorer.Preview.First",
              Integer.toString(ParquetFileInspector.PREVIEW_ROW_LIMIT)));
    } else {
      previewLabel.setText(
          BaseMessages.getString(PKG, "ParquetExplorer.Preview.Showing", Integer.toString(shown)));
    }

    IRowMeta rowMeta = inspection.getRowMeta();
    if (rowMeta == null || rowMeta.isEmpty()) {
      return;
    }
    ColumnInfo[] columns = new ColumnInfo[rowMeta.size()];
    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(i);
      columns[i] =
          new ColumnInfo(valueMeta.getName(), ColumnInfo.COLUMN_TYPE_TEXT, valueMeta.isNumeric());
      RowPreviewSupport.applyColumnMeta(columns[i], valueMeta);
    }
    previewView =
        new TableView(
            getVariables(),
            previewComposite,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            0,
            true,
            null,
            PropsUi.getInstance());
    previewView.setShowingBlueNullValues(true);
    previewView.setShortenDisplayedValues(true);
    previewView.setSortable(true);
    previewView.setReadonly(true);
    previewView.setLayoutData(
        new FormDataBuilder().top(previewLabel, PropsUi.getMargin()).bottom().fullWidth().result());
    RowPreviewSupport.installCellTooltips(previewView, rowMeta);

    List<Object[]> rows = inspection.getPreviewRows();
    for (int r = 0; r < rows.size(); r++) {
      TableItem item =
          r == 0 ? previewView.table.getItem(0) : new TableItem(previewView.table, SWT.NONE);
      item.setText(0, Integer.toString(r + 1));
      Object[] row = rows.get(r);
      if (row == null) {
        continue;
      }
      for (int c = 0; c < rowMeta.size(); c++) {
        String display;
        try {
          display = RowPreviewSupport.formatCell(rowMeta.getValueMeta(c), row[c]);
        } catch (HopValueException | ArrayIndexOutOfBoundsException e) {
          display = null;
        }
        if (display == null) {
          item.setText(c + 1, "<null>");
          item.setForeground(c + 1, GuiResource.getInstance().getColorBlue());
        } else {
          previewView.setCellValue(item, c + 1, display);
        }
      }
    }
    if (rows.isEmpty()) {
      previewView.table.removeAll();
    }
    if (!previewView.isDisposed()) {
      previewView.optWidth(true, 200);
    }
    previewComposite.layout(true, true);
  }

  private void showError(Composite composite, Exception e) {
    if (note != null && !note.isDisposed()) {
      note.setText(e.getMessage() == null ? e.toString() : e.getMessage());
    }
    if (hopGui.getShell() == null || hopGui.getShell().isDisposed()) {
      return;
    }
    new ErrorDialog(
        hopGui.getShell(),
        BaseMessages.getString(PKG, "ParquetExplorer.Error.Title"),
        BaseMessages.getString(PKG, "ParquetExplorer.Error.Message"),
        e);
  }

  private static String[] detail(String key, String value) {
    return new String[] {BaseMessages.getString(PKG, key), value == null ? "" : value};
  }

  private static String grouped(Long value) {
    return value == null ? "" : String.format(Locale.getDefault(), "%,d", value);
  }

  private static String number(int value) {
    return value < 0 ? "" : Integer.toString(value);
  }

  private static void fillRows(TableView table, List<String[]> rows) {
    if (table == null || table.isDisposed()) {
      return;
    }
    table.clearAll(false);
    if (rows.isEmpty()) {
      table.table.removeAll();
      return;
    }
    for (int i = 0; i < rows.size(); i++) {
      TableItem item = i == 0 ? table.table.getItem(0) : new TableItem(table.table, SWT.NONE);
      String[] values = rows.get(i);
      for (int c = 0; c < values.length; c++) {
        item.setText(c, values[c] == null ? "" : values[c]);
      }
    }
    table.optWidth(true);
  }
}
