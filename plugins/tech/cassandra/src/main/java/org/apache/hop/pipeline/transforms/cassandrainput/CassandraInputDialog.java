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
package org.apache.hop.pipeline.transforms.cassandrainput;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.databases.cassandra.metadata.CassandraConnection;
import org.apache.hop.databases.cassandra.spi.Connection;
import org.apache.hop.databases.cassandra.spi.Keyspace;
import org.apache.hop.databases.cassandra.util.CassandraUtils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.dialog.ShowMessageDialog;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** Dialog class for the CassandraInput transform. */
public class CassandraInputDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = CassandraInputMeta.class;

  private final CassandraInputMeta input;

  private MetaSelectionLine<CassandraConnection> wConnection;

  private TextVar wMaxLength;

  private Label wlPosition;

  private StyledTextComp wCql;

  private Button wExecuteForEachRow;

  public CassandraInputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta tr, String name) {
    super(parent, variables, (BaseTransformMeta) in, tr, name);
    input = (CassandraInputMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);

    props.setLook(shell);
    setShellImage(shell, input);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "CassandraInputDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Buttons go at the bottom
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wPreview = new Button(shell, SWT.PUSH);
    wPreview.setText(BaseMessages.getString(PKG, "System.Button.Preview"));
    wPreview.addListener(SWT.Selection, e -> preview());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wPreview, wCancel}, margin, null);

    // transform name line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(
        BaseMessages.getString(PKG, "CassandraInputDialog.transformName.Label"));
    props.setLook(wlTransformName);
    FormData fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);

    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    FormData fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    // Connection line
    wConnection =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            CassandraConnection.class,
            shell,
            SWT.NONE,
            BaseMessages.getString(PKG, "CassandraInputDialog.Connection.Label"),
            BaseMessages.getString(PKG, "CassandraInputDialog.Connection.Tooltip"));
    props.setLook(wConnection);
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment(0, 0);
    fdConnection.right = new FormAttachment(100, 0);
    fdConnection.top = new FormAttachment(wTransformName, margin);
    wConnection.setLayoutData(fdConnection);

    try {
      wConnection.fillItems();
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error listing Cassandra connection metadata objects", e);
    }

    // max length line
    Label wlMaxLength = new Label(shell, SWT.RIGHT);
    props.setLook(wlMaxLength);
    wlMaxLength.setText(
        BaseMessages.getString(PKG, "CassandraInputDialog.TransportMaxLength.Label"));
    FormData fdlMaxLength = new FormData();
    fdlMaxLength.left = new FormAttachment(0, 0);
    fdlMaxLength.top = new FormAttachment(wConnection, margin);
    fdlMaxLength.right = new FormAttachment(middle, -margin);
    wlMaxLength.setLayoutData(fdlMaxLength);

    wMaxLength = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wMaxLength);
    wMaxLength.addModifyListener(
        e -> wMaxLength.setToolTipText(variables.resolve(wMaxLength.getText())));
    FormData fdMaxLength = new FormData();
    fdMaxLength.right = new FormAttachment(100, 0);
    fdMaxLength.top = new FormAttachment(wlMaxLength, 0, SWT.CENTER);
    fdMaxLength.left = new FormAttachment(middle, 0);
    wMaxLength.setLayoutData(fdMaxLength);

    // execute for each row
    Label wlExecuteForEach = new Label(shell, SWT.RIGHT);
    props.setLook(wlExecuteForEach);
    wlExecuteForEach.setText(
        BaseMessages.getString(PKG, "CassandraInputDialog.ExecuteForEachRow.Label"));
    FormData fdlExecuteForEach = new FormData();
    fdlExecuteForEach.right = new FormAttachment(middle, -margin);
    fdlExecuteForEach.left = new FormAttachment(0, 0);
    fdlExecuteForEach.top = new FormAttachment(wMaxLength, margin);
    wlExecuteForEach.setLayoutData(fdlExecuteForEach);

    wExecuteForEachRow = new Button(shell, SWT.CHECK);
    props.setLook(wExecuteForEachRow);
    FormData fdExecuteForEach = new FormData();
    fdExecuteForEach.right = new FormAttachment(100, 0);
    fdExecuteForEach.left = new FormAttachment(middle, 0);
    fdExecuteForEach.top = new FormAttachment(wlExecuteForEach, 0, SWT.CENTER);
    wExecuteForEachRow.setLayoutData(fdExecuteForEach);

    // position label
    wlPosition = new Label(shell, SWT.NONE);
    props.setLook(wlPosition);
    FormData fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment(0, 0);
    fdlPosition.right = new FormAttachment(middle, -margin);
    fdlPosition.bottom = new FormAttachment(wOk, -margin);
    wlPosition.setLayoutData(fdlPosition);

    Button wbShowSchema = new Button(shell, SWT.PUSH);
    wbShowSchema.setText(BaseMessages.getString(PKG, "CassandraInputDialog.Schema.Button"));
    props.setLook(wbShowSchema);
    FormData fdbShowSchema = new FormData();
    fdbShowSchema.right = new FormAttachment(100, 0);
    fdbShowSchema.bottom = new FormAttachment(wOk, -margin);
    wbShowSchema.setLayoutData(fdbShowSchema);
    wbShowSchema.addListener(SWT.Selection, e -> popupSchemaInfo());

    // cql stuff
    Label wlCql = new Label(shell, SWT.NONE);
    props.setLook(wlCql);
    wlCql.setText(BaseMessages.getString(PKG, "CassandraInputDialog.CQL.Label"));
    FormData fdlCql = new FormData();
    fdlCql.left = new FormAttachment(0, 0);
    fdlCql.top = new FormAttachment(wExecuteForEachRow, margin);
    fdlCql.right = new FormAttachment(middle, -margin);
    wlCql.setLayoutData(fdlCql);

    wCql =
        new StyledTextComp(
            variables, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    props.setLook(wCql, Props.WIDGET_STYLE_FIXED);
    FormData fdCql = new FormData();
    fdCql.left = new FormAttachment(0, 0);
    fdCql.top = new FormAttachment(wlCql, margin);
    fdCql.right = new FormAttachment(100, -2 * margin);
    fdCql.bottom = new FormAttachment(wbShowSchema, -margin);
    wCql.setLayoutData(fdCql);
    wCql.addModifyListener(
        e -> {
          setPosition();
          wCql.setToolTipText(variables.resolve(wCql.getText()));
        });

    wCql.addKeyListener(
        new KeyAdapter() {
          @Override
          public void keyPressed(KeyEvent e) {
            setPosition();
          }

          @Override
          public void keyReleased(KeyEvent e) {
            setPosition();
          }
        });

    wCql.addFocusListener(
        new FocusAdapter() {
          @Override
          public void focusGained(FocusEvent e) {
            setPosition();
          }

          @Override
          public void focusLost(FocusEvent e) {
            setPosition();
          }
        });

    wCql.addMouseListener(
        new MouseAdapter() {
          @Override
          public void mouseDoubleClick(MouseEvent e) {
            setPosition();
          }

          @Override
          public void mouseDown(MouseEvent e) {
            setPosition();
          }

          @Override
          public void mouseUp(MouseEvent e) {
            setPosition();
          }
        });

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void getInfo(CassandraInputMeta meta) {
    meta.setConnectionName(wConnection.getText());
    meta.setMaxLength(wMaxLength.getText());
    meta.setCqlSelectQuery(wCql.getText());
    meta.setExecuteForEachIncomingRow(wExecuteForEachRow.getSelection());
  }

  protected void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText();
    getInfo(input);

    input.setChanged();

    dispose();
  }

  protected void cancel() {
    transformName = null;

    dispose();
  }

  protected void getData() {
    wConnection.setText(Const.NVL(input.getConnectionName(), ""));
    wMaxLength.setText(Const.NVL(input.getMaxLength(), ""));
    wExecuteForEachRow.setSelection(input.isExecuteForEachIncomingRow());

    if (!Utils.isEmpty(input.getCqlSelectQuery())) {
      wCql.setText(input.getCqlSelectQuery());
    }
  }

  protected void setPosition() {
    int lineNr = wCql.getLineNumber();
    int colNr = wCql.getColumnNumber();

    wlPosition.setText(
        BaseMessages.getString(
            PKG, "CassandraInputDialog.Position.Label", "" + lineNr, "" + colNr));
  }

  private boolean checkForUnresolved(CassandraInputMeta meta, String title) {
    String query = variables.resolve(meta.getCqlSelectQuery());

    boolean notOk = (query.contains("${") || query.contains("?{"));

    if (notOk) {
      ShowMessageDialog smd =
          new ShowMessageDialog(
              shell,
              SWT.ICON_WARNING | SWT.OK,
              title,
              BaseMessages.getString(
                  PKG,
                  "CassandraInputDialog.Warning.Message.CassandraQueryContainsUnresolvedVarsFieldSubs"));
      smd.open();
    }

    return !notOk;
  }

  private void preview() {
    CassandraInputMeta oneMeta = new CassandraInputMeta();
    getInfo(oneMeta);

    // Turn off execute for each incoming row (if set). Query is still going to
    // be stuffed if the user has specified field replacement (i.e. ?{...}) in
    // the query string
    oneMeta.setExecuteForEachIncomingRow(false);

    if (!checkForUnresolved(
        oneMeta,
        BaseMessages.getString(
            PKG,
            "CassandraInputDialog.Warning.Message.CassandraQueryContainsUnresolvedVarsFieldSubs.PreviewTitle"))) {
      return;
    }

    PipelineMeta previewMeta =
        PipelinePreviewFactory.generatePreviewPipeline(
            pipelineMeta.getMetadataProvider(), oneMeta, wTransformName.getText());

    EnterNumberDialog numberDialog =
        new EnterNumberDialog(
            shell,
            props.getDefaultPreviewSize(),
            BaseMessages.getString(PKG, "CassandraInputDialog.PreviewSize.DialogTitle"),
            BaseMessages.getString(PKG, "CassandraInputDialog.PreviewSize.DialogMessage"));

    int previewSize = numberDialog.open();
    if (previewSize > 0) {
      PipelinePreviewProgressDialog progressDialog =
          new PipelinePreviewProgressDialog(
              shell,
              variables,
              previewMeta,
              new String[] {wTransformName.getText()},
              new int[] {previewSize});
      progressDialog.open();

      Pipeline pipeline = progressDialog.getPipeline();
      String loggingText = progressDialog.getLoggingText();

      if (!progressDialog.isCancelled()) {
        if (pipeline.getResult() != null && pipeline.getResult().getNrErrors() > 0) {
          EnterTextDialog etd =
              new EnterTextDialog(
                  shell,
                  BaseMessages.getString(PKG, "System.Dialog.PreviewError.Title"),
                  BaseMessages.getString(PKG, "System.Dialog.PreviewError.Message"),
                  loggingText,
                  true);
          etd.setReadOnly();
          etd.open();
        }
      }
      PreviewRowsDialog prd =
          new PreviewRowsDialog(
              shell,
              variables,
              SWT.NONE,
              wTransformName.getText(),
              progressDialog.getPreviewRowsMeta(wTransformName.getText()),
              progressDialog.getPreviewRows(wTransformName.getText()),
              loggingText);
      prd.open();
    }
  }

  protected void popupSchemaInfo() {

    Connection conn = null;
    Keyspace kSpace = null;
    try {
      CassandraConnection cassandraConnection =
          metadataProvider
              .getSerializer(CassandraConnection.class)
              .load(variables.resolve(wConnection.getText()));

      try {
        conn = cassandraConnection.createConnection(variables, false);
        kSpace = cassandraConnection.lookupKeyspace(conn, variables);
      } catch (Exception e) {
        logError(
            BaseMessages.getString(
                    PKG, "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Message")
                + ":\n\n"
                + e.getLocalizedMessage(),
            e);
        new ErrorDialog(
            shell,
            BaseMessages.getString(
                PKG, "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Title"),
            BaseMessages.getString(
                    PKG, "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Message")
                + ":\n\n"
                + e.getLocalizedMessage(),
            e);
        return;
      }

      String cqlText = variables.resolve(wCql.getText());
      String table = CassandraUtils.getTableNameFromCQLSelectQuery(cqlText);
      if (Utils.isEmpty(table)) {
        throw new Exception(
            BaseMessages.getString(PKG, "CassandraInput.Error.NoFromClauseInQuery"));
      }

      if (!kSpace.tableExists(table)) {
        throw new Exception(
            BaseMessages.getString(
                PKG,
                "CassandraInput.Error.NonExistentTable",
                CassandraUtils.removeQuotes(table),
                cassandraConnection.getKeyspace()));
      }

      String schemaDescription = kSpace.getTableMetaData(table).describe();
      ShowMessageDialog smd =
          new ShowMessageDialog(
              shell, SWT.ICON_INFORMATION | SWT.OK, "Schema info", schemaDescription, true);
      smd.open();
    } catch (Exception e1) {
      logError(
          BaseMessages.getString(PKG, "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Message")
              + ":\n\n"
              + e1.getMessage(),
          e1);
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Title"),
          BaseMessages.getString(PKG, "CassandraInputDialog.Error.ProblemGettingSchemaInfo.Message")
              + ":\n\n"
              + e1.getMessage(),
          e1);
    } finally {
      if (conn != null) {
        try {
          conn.closeConnection();
        } catch (Exception e) {
          log.logError(e.getLocalizedMessage(), e);
          // TODO popup another error dialog
        }
      }
    }
  }
}
