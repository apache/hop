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

package org.apache.hop.ui.datacatalog;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.datacatalog.CatalogDataset;
import org.apache.hop.datacatalog.CatalogFieldTypes;
import org.apache.hop.datacatalog.CatalogSchemas;
import org.apache.hop.datacatalog.DataCatalog;
import org.apache.hop.datacatalog.ISchemaCapability.CatalogField;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Shell;

/**
 * Asks for a data catalog and a dataset in it, and reads the fields of that dataset, for a
 * transform dialog to fill its field table with. The transform keeps its own field definitions:
 * nothing is read from the catalog when the pipeline runs.
 */
public class CatalogFieldsDialog {

  private static final Class<?> PKG = CatalogFieldsDialog.class;

  private final Shell parent;
  private final IVariables variables;
  private final IHopMetadataProvider metadataProvider;
  private final CatalogFieldsSource source;
  private IRowMeta fields;

  /**
   * @param dataset the dataset to start from, typically the file or table of the transform
   */
  public CatalogFieldsDialog(
      Shell parent,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      CatalogDataset dataset) {
    this.parent = parent;
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.source = new CatalogFieldsSource();
    source.setCatalogName(defaultCatalogName(metadataProvider));
    if (dataset != null) {
      source.setDatasetType(dataset.getType());
      source.setConnection(dataset.getConnection());
      source.setSchema(dataset.getSchema());
      source.setName(dataset.getName());
    }
  }

  /** True when the project has a data catalog to get fields from. */
  public static boolean isAvailable(IHopMetadataProvider metadataProvider) {
    return !catalogNames(metadataProvider).isEmpty();
  }

  /**
   * Fills a field table with the fields of a dataset in a data catalog. The person picks the
   * catalog and dataset, and then whether to add the new fields, add all of them or replace the
   * table.
   */
  public static void getFields(
      Shell parent,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      CatalogDataset dataset,
      TableView table,
      int nameColumn,
      int typeColumn,
      int lengthColumn,
      int precisionColumn) {
    IRowMeta rowMeta = new CatalogFieldsDialog(parent, variables, metadataProvider, dataset).open();
    if (rowMeta == null) {
      return;
    }
    BaseTransformDialog.getFieldsFromPrevious(
        rowMeta,
        table,
        nameColumn,
        new int[] {nameColumn},
        new int[] {typeColumn},
        lengthColumn,
        precisionColumn,
        true,
        null);
  }

  /** The fields of the dataset, or null when the dialog was cancelled. */
  public IRowMeta open() {
    Shell shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.APPLICATION_MODAL);
    shell.setMinimumSize(520, 320);
    PropsUi.setLook(shell);
    shell.setText(BaseMessages.getString(PKG, "CatalogFieldsDialog.Title"));
    FormLayout layout = new FormLayout();
    int margin = PropsUi.getMargin();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(layout);

    Button ok = new Button(shell, SWT.PUSH);
    ok.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    Button cancel = new Button(shell, SWT.PUSH);
    cancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {ok, cancel}, margin, null);
    shell.setDefaultButton(ok);

    GuiCompositeWidgets widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell, variables, null, ok, CatalogFieldsSource.PARENT_ID, source);

    ok.addListener(SWT.Selection, e -> accept(shell, widgets));
    cancel.addListener(SWT.Selection, e -> shell.dispose());
    BaseDialog.defaultShellHandling(shell, c -> accept(shell, widgets), c -> shell.dispose());
    return fields;
  }

  private void accept(Shell shell, GuiCompositeWidgets widgets) {
    widgets.getWidgetsContents(source, CatalogFieldsSource.PARENT_ID);
    shell.setCursor(shell.getDisplay().getSystemCursor(SWT.CURSOR_WAIT));
    try {
      List<CatalogField> catalogFields =
          CatalogSchemas.read(
              variables, metadataProvider, source.getCatalogName(), source.dataset());
      fields = CatalogFieldTypes.toRowMeta(catalogFields);
      LastCatalog lastCatalog = lastCatalog();
      if (lastCatalog != null) {
        lastCatalog.name = source.getCatalogName();
      }
      shell.dispose();
    } catch (HopException | RuntimeException e) {
      // A catalog plugin can fail in ways it doesn't wrap; show it and keep the dialog open
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "CatalogFieldsDialog.Error.Title"),
          BaseMessages.getString(PKG, "CatalogFieldsDialog.Error.Message"),
          e);
    } finally {
      if (!shell.isDisposed()) {
        shell.setCursor(null);
      }
    }
  }

  private static List<String> catalogNames(IHopMetadataProvider metadataProvider) {
    if (metadataProvider == null) {
      return List.of();
    }
    try {
      return metadataProvider.getSerializer(DataCatalog.class).listObjectNames();
    } catch (HopException e) {
      return List.of();
    }
  }

  private static String defaultCatalogName(IHopMetadataProvider metadataProvider) {
    List<String> names = catalogNames(metadataProvider);
    LastCatalog lastCatalog = lastCatalog();
    String lastName = lastCatalog == null ? null : lastCatalog.name;
    if (lastName != null && names.contains(lastName)) {
      return lastName;
    }
    return names.isEmpty() ? null : names.get(0);
  }

  /**
   * The catalog picked last in this Hop GUI session. Hop Web has a Hop GUI per browser session, so
   * one user's choice is not another user's default. Null without a Hop GUI.
   */
  private static LastCatalog lastCatalog() {
    HopGui hopGui = HopGui.peekInstance();
    return hopGui == null ? null : hopGui.getSessionSingleton(LastCatalog.class, LastCatalog::new);
  }

  /** The catalog picked last in a session, to start from the next time. */
  private static final class LastCatalog {
    private volatile String name;
  }
}
