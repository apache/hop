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

package org.apache.hop.pipeline.transforms.splunkinput;

import com.splunk.Args;
import com.splunk.JobArgs;
import com.splunk.ResultsReaderXml;
import com.splunk.Service;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.splunk.SplunkConnection;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class SplunkInputDialog extends BaseTransformDialog {

  private static final Class<?> PKG =
      SplunkInputMeta.class; // for i18n purposes, needed by Translator2!!

  private MetaSelectionLine<SplunkConnection> wConnection;

  private Text wQuery;

  private TableView wReturns;

  private final SplunkInputMeta input;

  public SplunkInputDialog(
      Shell parent,
      IVariables variables,
      SplunkInputMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "SplunkInputDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).preview(e -> preview()).cancel(e -> cancel()).build();

    changed = input.hasChanged();

    ScrolledComposite wScrolledComposite =
        new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    FormLayout scFormLayout = new FormLayout();
    wScrolledComposite.setLayout(scFormLayout);
    FormData fdSComposite = new FormData();
    fdSComposite.left = new FormAttachment(0, 0);
    fdSComposite.right = new FormAttachment(100, 0);
    fdSComposite.top = new FormAttachment(wSpacer, 0);
    fdSComposite.bottom = new FormAttachment(wOk, -margin);
    wScrolledComposite.setLayoutData(fdSComposite);

    Composite wComposite = new Composite(wScrolledComposite, SWT.NONE);
    PropsUi.setLook(wComposite);
    FormData fdComposite = new FormData();
    fdComposite.left = new FormAttachment(0, 0);
    fdComposite.right = new FormAttachment(100, 0);
    fdComposite.top = new FormAttachment(0, 0);
    fdComposite.bottom = new FormAttachment(100, 0);
    wComposite.setLayoutData(fdComposite);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    wComposite.setLayout(formLayout);

    Control lastControl = wSpacer;

    wConnection =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            SplunkConnection.class,
            wComposite,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            "Splunk Connection",
            "Select, create or edit a Splunk Connection");
    PropsUi.setLook(wConnection);
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment(0, 0);
    fdConnection.right = new FormAttachment(100, 0);
    fdConnection.top = new FormAttachment(lastControl, margin);
    wConnection.setLayoutData(fdConnection);
    lastControl = wConnection;

    Label wlQuery = new Label(wComposite, SWT.LEFT);
    wlQuery.setText("Query:");
    PropsUi.setLook(wlQuery);
    FormData fdlQuery = new FormData();
    fdlQuery.left = new FormAttachment(0, 0);
    fdlQuery.right = new FormAttachment(middle, -margin);
    fdlQuery.top = new FormAttachment(lastControl, margin);
    wlQuery.setLayoutData(fdlQuery);
    wQuery = new Text(wComposite, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    wQuery.setFont(GuiResource.getInstance().getFontFixed());
    PropsUi.setLook(wQuery);
    FormData fdQuery = new FormData();
    fdQuery.left = new FormAttachment(0, 0);
    fdQuery.right = new FormAttachment(100, 0);
    fdQuery.top = new FormAttachment(wlQuery, margin);
    fdQuery.bottom = new FormAttachment(60, 0);
    fdQuery.height = 200;
    wQuery.setLayoutData(fdQuery);
    lastControl = wQuery;

    // Table: return field name and type
    //
    ColumnInfo[] returnColumns =
        new ColumnInfo[] {
          new ColumnInfo("Field name", ColumnInfo.COLUMN_TYPE_TEXT, false),
          new ColumnInfo("Splunk name", ColumnInfo.COLUMN_TYPE_TEXT, false),
          new ColumnInfo(
              "Return type",
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getAllValueMetaNames(),
              false),
          new ColumnInfo("Length", ColumnInfo.COLUMN_TYPE_TEXT, false),
          new ColumnInfo("Format", ColumnInfo.COLUMN_TYPE_TEXT, false),
        };

    Label wlReturns = new Label(wComposite, SWT.LEFT);
    wlReturns.setText("Returns");
    PropsUi.setLook(wlReturns);
    FormData fdlReturns = new FormData();
    fdlReturns.left = new FormAttachment(0, 0);
    fdlReturns.right = new FormAttachment(middle, -margin);
    fdlReturns.top = new FormAttachment(lastControl, margin);
    wlReturns.setLayoutData(fdlReturns);

    Button wbGetReturnFields = new Button(wComposite, SWT.PUSH);
    wbGetReturnFields.setText("Get Output Fields");
    FormData fdbGetReturnFields = new FormData();
    fdbGetReturnFields.right = new FormAttachment(100, 0);
    fdbGetReturnFields.top = new FormAttachment(lastControl, margin);
    wbGetReturnFields.setLayoutData(fdbGetReturnFields);
    wbGetReturnFields.addListener(SWT.Selection, e -> getReturnValues());
    lastControl = wbGetReturnFields;

    wReturns =
        new TableView(
            variables,
            wComposite,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            returnColumns,
            input.getReturnValues().size(),
            null,
            props);
    PropsUi.setLook(wReturns);
    FormData fdReturns = new FormData();
    fdReturns.left = new FormAttachment(0, 0);
    fdReturns.right = new FormAttachment(100, 0);
    fdReturns.top = new FormAttachment(lastControl, margin);
    fdReturns.bottom = new FormAttachment(100, -margin);
    wReturns.setLayoutData(fdReturns);

    wComposite.pack();
    Rectangle bounds = wComposite.getBounds();

    wScrolledComposite.setContent(wComposite);

    wScrolledComposite.setExpandHorizontal(true);
    wScrolledComposite.setExpandVertical(true);
    wScrolledComposite.setMinWidth(bounds.width);
    wScrolledComposite.setMinHeight(bounds.height);

    getData();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  public void getData() {
    wConnection.setText(Const.NVL(input.getConnectionName(), ""));

    // List of connections...
    //
    try {
      List<String> elementNames =
          metadataProvider.getSerializer(SplunkConnection.class).listObjectNames();
      Collections.sort(elementNames);
      wConnection.setItems(elementNames.toArray(new String[0]));
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Unable to list Splunk connections", e);
    }

    wQuery.setText(Const.NVL(input.getQuery(), ""));

    for (int i = 0; i < input.getReturnValues().size(); i++) {
      ReturnValue returnValue = input.getReturnValues().get(i);
      TableItem item = wReturns.table.getItem(i);
      item.setText(1, Const.NVL(returnValue.getName(), ""));
      item.setText(2, Const.NVL(returnValue.getSplunkName(), ""));
      item.setText(3, Const.NVL(returnValue.getType(), ""));
      item.setText(4, returnValue.getLength() < 0 ? "" : Integer.toString(returnValue.getLength()));
      item.setText(5, Const.NVL(returnValue.getFormat(), ""));
    }
    wReturns.removeEmptyRows();
    wReturns.setRowNums();
    wReturns.optWidth(true);
  }

  private void ok() {
    if (StringUtils.isEmpty(wTransformName.getText())) {
      return;
    }
    transformName = wTransformName.getText(); // return value
    getInfo(input);
    dispose();
  }

  private void getInfo(SplunkInputMeta meta) {
    meta.setConnectionName(wConnection.getText());
    meta.setQuery(wQuery.getText());

    List<ReturnValue> returnValues = new ArrayList<>();
    for (int i = 0; i < wReturns.nrNonEmpty(); i++) {
      TableItem item = wReturns.getNonEmpty(i);
      String name = item.getText(1);
      String splunkName = item.getText(2);
      String type = item.getText(3);
      int length = Const.toInt(item.getText(4), -1);
      String format = item.getText(5);
      returnValues.add(new ReturnValue(name, splunkName, type, length, format));
    }
    meta.setReturnValues(returnValues);
  }

  private synchronized void preview() {
    SplunkInputMeta oneMeta = new SplunkInputMeta();
    this.getInfo(oneMeta);
    PipelineMeta previewMeta =
        PipelinePreviewFactory.generatePreviewPipeline(
            metadataProvider, oneMeta, this.wTransformName.getText());
    EnterNumberDialog numberDialog =
        new EnterNumberDialog(
            this.shell,
            this.props.getDefaultPreviewSize(),
            BaseMessages.getString(PKG, "QueryDialog.PreviewSize.DialogTitle"),
            BaseMessages.getString(PKG, "QueryDialog.PreviewSize.DialogMessage"));
    int previewSize = numberDialog.open();
    if (previewSize > 0) {
      PipelinePreviewProgressDialog progressDialog =
          new PipelinePreviewProgressDialog(
              this.shell,
              variables,
              previewMeta,
              new String[] {this.wTransformName.getText()},
              new int[] {previewSize});
      progressDialog.open();
      Pipeline pipeline = progressDialog.getPipeline();
      String loggingText = progressDialog.getLoggingText();
      if (!progressDialog.isCancelled()
          && pipeline.getResult() != null
          && pipeline.getResult().getNrErrors() > 0L) {
        EnterTextDialog etd =
            new EnterTextDialog(
                this.shell,
                BaseMessages.getString(PKG, "System.Dialog.PreviewError.Title", new String[0]),
                BaseMessages.getString(PKG, "System.Dialog.PreviewError.Message", new String[0]),
                loggingText,
                true);
        etd.setReadOnly();
        etd.open();
      }

      PreviewRowsDialog prd =
          new PreviewRowsDialog(
              this.shell,
              variables,
              0,
              this.wTransformName.getText(),
              progressDialog.getPreviewRowsMeta(this.wTransformName.getText()),
              progressDialog.getPreviewRows(this.wTransformName.getText()),
              loggingText);
      prd.open();
    }
  }

  private void getReturnValues() {

    try {
      IHopMetadataSerializer<SplunkConnection> serializer =
          metadataProvider.getSerializer(SplunkConnection.class);
      SplunkConnection splunkConnection = serializer.load(variables.resolve(wConnection.getText()));
      Service service = Service.connect(splunkConnection.getServiceArgs(variables));
      Args args = new Args();
      args.put("connection_mode", JobArgs.ExecutionMode.BLOCKING.name());

      InputStream eventsStream = service.oneshotSearch(variables.resolve(wQuery.getText()), args);

      Set<String> detectedKeys = new HashSet<>();
      try {
        ResultsReaderXml resultsReader = new ResultsReaderXml(eventsStream);
        HashMap<String, String> event;
        int nrScanned = 0;
        while ((event = resultsReader.getNextEvent()) != null) {
          for (String key : event.keySet()) {
            detectedKeys.add(key);
          }
          nrScanned++;
          if (nrScanned > 10) {
            break;
          }
        }
      } finally {
        eventsStream.close();
      }

      for (String detectedKey : detectedKeys) {
        TableItem item = new TableItem(wReturns.table, SWT.NONE);
        item.setText(1, detectedKey);
        item.setText(2, detectedKey);
        item.setText(3, "String");
      }
      wReturns.removeEmptyRows();
      wReturns.setRowNums();
      wReturns.optWidth(true);

    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting fields from Splunk query", e);
    }
  }
}
