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
 *
 */
package org.apache.hop.pipeline.transforms.googleanalytics;

import com.google.analytics.data.v1beta.BetaAnalyticsDataClient;
import com.google.analytics.data.v1beta.BetaAnalyticsDataSettings;
import com.google.analytics.data.v1beta.DateRange;
import com.google.analytics.data.v1beta.Dimension;
import com.google.analytics.data.v1beta.DimensionHeader;
import com.google.analytics.data.v1beta.Metric;
import com.google.analytics.data.v1beta.MetricHeader;
import com.google.analytics.data.v1beta.MetricType;
import com.google.analytics.data.v1beta.RunReportRequest;
import com.google.analytics.data.v1beta.RunReportResponse;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class GoogleAnalyticsDialog extends BaseTransformDialog {

  private static final Class<?> PKG = GoogleAnalyticsMeta.class;
  public static final String CONST_GOOGLE_ANALYTICS_DIALOG_QUERY_REFERENCE_LABEL =
      "GoogleAnalyticsDialog.Query.Reference.Label";

  private GoogleAnalyticsMeta input;

  private TextVar wOauthAccount;

  private Button fileChooser;
  private TextVar keyFilename;

  private TableView wFields;

  private TextVar wQuStartDate;

  private TextVar wQuEndDate;

  private TextVar wQuDimensions;

  private TextVar wQuMetrics;

  private TextVar wQuSort;

  private Group gConnect;

  private TextVar wGaAppName;

  private Text wLimit;

  private TextVar wGaPropertyId;

  private ModifyListener lsMod;

  static final String REFERENCE_SORT_URI =
      "https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/OrderBy";
  static final String REFERENCE_METRICS_URI =
      "https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema#metrics";
  static final String REFERENCE_DIMENSIONS_URI =
      "https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema#dimensions";
  static final String REFERENCE_DIMENSION_AND_METRIC_URI =
      "https://support.google.com/analytics/answer/9143382?hl=en";
  static final String REFERENCE_METRICAGGS_URI =
      "https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/MetricAggregation";

  public GoogleAnalyticsDialog(
      Shell parent,
      IVariables variables,
      GoogleAnalyticsMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    setInput(transformMeta);
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "GoogleAnalyticsDialog.Shell.Title"));

    buildButtonBar()
        .ok(e -> ok())
        .get(e -> getFields())
        .preview(e -> preview())
        .cancel(e -> cancel())
        .build();

    lsMod = e -> getInput().setChanged();
    backupChanged = getInput().hasChanged();

    /*************************************************
     * // GOOGLE ANALYTICS CONNECTION GROUP
     *************************************************/

    gConnect = new Group(shell, SWT.SHADOW_ETCHED_IN);
    gConnect.setText(BaseMessages.getString(PKG, "GoogleAnalyticsDialog.ConnectGroup.Label"));
    FormLayout gConnectLayout = new FormLayout();
    gConnectLayout.marginWidth = 3;
    gConnectLayout.marginHeight = 3;
    gConnect.setLayout(gConnectLayout);
    PropsUi.setLook(gConnect);

    FormData fdConnect = new FormData();
    fdConnect.left = new FormAttachment(0, 0);
    fdConnect.right = new FormAttachment(100, 0);
    fdConnect.top = new FormAttachment(wSpacer, margin);
    gConnect.setLayoutData(fdConnect);

    // Google Analytics app name
    Label wlGaAppName = new Label(gConnect, SWT.RIGHT);
    wlGaAppName.setText(BaseMessages.getString(PKG, "GoogleAnalyticsDialog.AppName.Label"));
    PropsUi.setLook(wlGaAppName);
    FormData fdlGaAppName = new FormData();
    fdlGaAppName.top = new FormAttachment(0, margin);
    fdlGaAppName.left = new FormAttachment(0, 0);
    fdlGaAppName.right = new FormAttachment(middle, -margin);
    wlGaAppName.setLayoutData(fdlGaAppName);
    wGaAppName = new TextVar(variables, gConnect, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wGaAppName.addModifyListener(lsMod);
    wGaAppName.setToolTipText(BaseMessages.getString(PKG, "GoogleAnalyticsDialog.AppName.Tooltip"));
    PropsUi.setLook(wGaAppName);
    FormData fdGaAppName = new FormData();
    fdGaAppName.top = new FormAttachment(wSpacer, margin);
    fdGaAppName.left = new FormAttachment(middle, 0);
    fdGaAppName.right = new FormAttachment(100, 0);
    wGaAppName.setLayoutData(fdGaAppName);

    createOauthServiceCredentialsControls();

    /*************************************************
     * // GOOGLE ANALYTICS QUERY GROUP
     *************************************************/

    Group gQuery = new Group(shell, SWT.SHADOW_ETCHED_IN);
    gQuery.setText(BaseMessages.getString(PKG, "GoogleAnalyticsDialog.QueryGroup.Label"));
    FormLayout gQueryLayout = new FormLayout();
    gQueryLayout.marginWidth = 3;
    gQueryLayout.marginHeight = 3;
    gQuery.setLayout(gQueryLayout);
    PropsUi.setLook(gQuery);

    // query start date
    Label wlQuStartDate = new Label(gQuery, SWT.RIGHT);
    wlQuStartDate.setText(
        BaseMessages.getString(PKG, "GoogleAnalyticsDialog.Query.StartDate.Label"));
    PropsUi.setLook(wlQuStartDate);
    FormData fdlQuStartDate = new FormData();
    fdlQuStartDate.top = new FormAttachment(0, margin);
    fdlQuStartDate.left = new FormAttachment(0, 0);
    fdlQuStartDate.right = new FormAttachment(middle, -margin);
    wlQuStartDate.setLayoutData(fdlQuStartDate);
    wQuStartDate = new TextVar(variables, gQuery, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wQuStartDate.addModifyListener(lsMod);
    wQuStartDate.setToolTipText(
        BaseMessages.getString(PKG, "GoogleAnalyticsDialog.Query.StartDate.Tooltip"));
    PropsUi.setLook(wQuStartDate);
    FormData fdQuStartDate = new FormData();
    fdQuStartDate.top = new FormAttachment(0, margin);
    fdQuStartDate.left = new FormAttachment(middle, 0);
    fdQuStartDate.right = new FormAttachment(100, 0);
    wQuStartDate.setLayoutData(fdQuStartDate);

    // query end date
    Label wlQuEndDate = new Label(gQuery, SWT.RIGHT);
    wlQuEndDate.setText(BaseMessages.getString(PKG, "GoogleAnalyticsDialog.Query.EndDate.Label"));
    PropsUi.setLook(wlQuEndDate);
    FormData fdlQuEndDate = new FormData();
    fdlQuEndDate.top = new FormAttachment(wQuStartDate, margin);
    fdlQuEndDate.left = new FormAttachment(0, 0);
    fdlQuEndDate.right = new FormAttachment(middle, -margin);
    wlQuEndDate.setLayoutData(fdlQuEndDate);
    wQuEndDate = new TextVar(variables, gQuery, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wQuEndDate.addModifyListener(lsMod);
    wQuEndDate.setToolTipText(
        BaseMessages.getString(PKG, "GoogleAnalyticsDialog.Query.EndDate.Tooltip"));
    PropsUi.setLook(wQuEndDate);
    FormData fdQuEndDate = new FormData();
    fdQuEndDate.top = new FormAttachment(wQuStartDate, margin);
    fdQuEndDate.left = new FormAttachment(middle, 0);
    fdQuEndDate.right = new FormAttachment(100, 0);
    wQuEndDate.setLayoutData(fdQuEndDate);

    // query dimensions
    Label wlQuDimensions = new Label(gQuery, SWT.RIGHT);
    wlQuDimensions.setText(
        BaseMessages.getString(PKG, "GoogleAnalyticsDialog.Query.Dimensions.Label"));
    PropsUi.setLook(wlQuDimensions);
    FormData fdlQuDimensions = new FormData();
    fdlQuDimensions.top = new FormAttachment(wQuEndDate, margin);
    fdlQuDimensions.left = new FormAttachment(0, 0);
    fdlQuDimensions.right = new FormAttachment(middle, -margin);
    wlQuDimensions.setLayoutData(fdlQuDimensions);
    wQuDimensions = new TextVar(variables, gQuery, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wQuDimensions.addModifyListener(lsMod);
    wQuDimensions.setToolTipText(
        BaseMessages.getString(PKG, "GoogleAnalyticsDialog.Query.Dimensions.Tooltip"));
    PropsUi.setLook(wQuDimensions);

    Link wQuDimensionsReference = new Link(gQuery, SWT.SINGLE);

    wQuDimensionsReference.setText(
        BaseMessages.getString(PKG, CONST_GOOGLE_ANALYTICS_DIALOG_QUERY_REFERENCE_LABEL));
    PropsUi.setLook(wQuDimensionsReference);
    wQuDimensionsReference.addListener(
        SWT.Selection, ev -> BareBonesBrowserLaunch.openURL(REFERENCE_DIMENSIONS_URI));

    wQuDimensionsReference.pack(true);

    FormData fdQuDimensions = new FormData();
    fdQuDimensions.top = new FormAttachment(wQuEndDate, margin);
    fdQuDimensions.left = new FormAttachment(middle, 0);
    fdQuDimensions.right =
        new FormAttachment(100, -wQuDimensionsReference.getBounds().width - margin);
    wQuDimensions.setLayoutData(fdQuDimensions);

    FormData fdQuDimensionsReference = new FormData();
    fdQuDimensionsReference.top = new FormAttachment(wQuEndDate, margin);
    fdQuDimensionsReference.left = new FormAttachment(wQuDimensions, 0);
    fdQuDimensionsReference.right = new FormAttachment(100, 0);
    wQuDimensionsReference.setLayoutData(fdQuDimensionsReference);

    // query Metrics
    Label wlQuMetrics = new Label(gQuery, SWT.RIGHT);
    wlQuMetrics.setText(BaseMessages.getString(PKG, "GoogleAnalyticsDialog.Query.Metrics.Label"));
    PropsUi.setLook(wlQuMetrics);
    FormData fdlQuMetrics = new FormData();
    fdlQuMetrics.top = new FormAttachment(wQuDimensions, margin);
    fdlQuMetrics.left = new FormAttachment(0, 0);
    fdlQuMetrics.right = new FormAttachment(middle, -margin);
    wlQuMetrics.setLayoutData(fdlQuMetrics);
    wQuMetrics = new TextVar(variables, gQuery, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wQuMetrics.addModifyListener(lsMod);
    wQuMetrics.setToolTipText(
        BaseMessages.getString(PKG, "GoogleAnalyticsDialog.Query.Metrics.Tooltip"));
    PropsUi.setLook(wQuMetrics);

    Link wQuMetricsReference = new Link(gQuery, SWT.SINGLE);
    wQuMetricsReference.setText(
        BaseMessages.getString(PKG, CONST_GOOGLE_ANALYTICS_DIALOG_QUERY_REFERENCE_LABEL));
    PropsUi.setLook(wQuMetricsReference);
    wQuMetricsReference.addListener(
        SWT.Selection, ev -> BareBonesBrowserLaunch.openURL(REFERENCE_METRICS_URI));

    wQuMetricsReference.pack(true);

    FormData fdQuMetrics = new FormData();
    fdQuMetrics.top = new FormAttachment(wQuDimensions, margin);
    fdQuMetrics.left = new FormAttachment(middle, 0);
    fdQuMetrics.right = new FormAttachment(100, -wQuMetricsReference.getBounds().width - margin);
    wQuMetrics.setLayoutData(fdQuMetrics);

    FormData fdQuMetricsReference = new FormData();
    fdQuMetricsReference.top = new FormAttachment(wQuDimensions, margin);
    fdQuMetricsReference.left = new FormAttachment(wQuMetrics, 0);
    fdQuMetricsReference.right = new FormAttachment(100, 0);
    wQuMetricsReference.setLayoutData(fdQuMetricsReference);

    // query Sort
    Label wlQuSort = new Label(gQuery, SWT.RIGHT);
    wlQuSort.setText(BaseMessages.getString(PKG, "GoogleAnalyticsDialog.Query.Sort.Label"));
    PropsUi.setLook(wlQuSort);
    FormData fdlQuSort = new FormData();
    fdlQuSort.top = new FormAttachment(wQuMetrics, margin);
    fdlQuSort.left = new FormAttachment(0, 0);
    fdlQuSort.right = new FormAttachment(middle, -margin);
    wlQuSort.setLayoutData(fdlQuSort);
    wQuSort = new TextVar(variables, gQuery, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wQuSort.addModifyListener(lsMod);
    wQuSort.setToolTipText(BaseMessages.getString(PKG, "GoogleAnalyticsDialog.Query.Sort.Tooltip"));
    PropsUi.setLook(wQuSort);

    Link wQuSortReference = new Link(gQuery, SWT.SINGLE);
    wQuSortReference.setText(
        BaseMessages.getString(PKG, CONST_GOOGLE_ANALYTICS_DIALOG_QUERY_REFERENCE_LABEL));
    PropsUi.setLook(wQuSortReference);
    wQuSortReference.addListener(
        SWT.Selection, ev -> BareBonesBrowserLaunch.openURL(REFERENCE_SORT_URI));

    wQuSortReference.pack(true);

    FormData fdQuSort = new FormData();
    fdQuSort.top = new FormAttachment(wQuMetrics, margin);
    fdQuSort.left = new FormAttachment(middle, 0);
    fdQuSort.right = new FormAttachment(100, -wQuSortReference.getBounds().width - margin);
    wQuSort.setLayoutData(fdQuSort);

    FormData fdQuSortReference = new FormData();
    fdQuSortReference.top = new FormAttachment(wQuMetrics, margin);
    fdQuSortReference.left = new FormAttachment(wQuSort, 0);
    fdQuSortReference.right = new FormAttachment(100, 0);
    wQuSortReference.setLayoutData(fdQuSortReference);

    FormData fdQueryGroup = new FormData();
    fdQueryGroup.left = new FormAttachment(0, 0);
    fdQueryGroup.right = new FormAttachment(100, 0);
    fdQueryGroup.top = new FormAttachment(gConnect, margin);
    gQuery.setLayoutData(fdQueryGroup);

    gQuery.setTabList(new Control[] {wQuStartDate, wQuEndDate, wQuDimensions, wQuMetrics, wQuSort});

    // Limit input ...
    Label wlLimit = new Label(shell, SWT.RIGHT);
    wlLimit.setText(BaseMessages.getString(PKG, "GoogleAnalyticsDialog.LimitSize.Label"));
    PropsUi.setLook(wlLimit);
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment(0, 0);
    fdlLimit.right = new FormAttachment(middle, -margin);
    fdlLimit.bottom = new FormAttachment(wOk, -margin);
    wlLimit.setLayoutData(fdlLimit);
    wLimit = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wLimit.setToolTipText(BaseMessages.getString(PKG, "GoogleAnalyticsDialog.LimitSize.Tooltip"));
    PropsUi.setLook(wLimit);
    wLimit.addModifyListener(lsMod);
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment(middle, 0);
    fdLimit.right = new FormAttachment(100, 0);
    fdLimit.top = new FormAttachment(wlLimit, 0, SWT.CENTER);

    wLimit.setLayoutData(fdLimit);

    /*************************************************
     * // KEY / LOOKUP TABLE
     *************************************************/

    // lookup fields settings widgets
    Link wlFields = new Link(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "GoogleAnalyticsDialog.Return.Label"));
    PropsUi.setLook(wlFields);
    wlFields.addListener(
        SWT.Selection, ev -> BareBonesBrowserLaunch.openURL(REFERENCE_DIMENSION_AND_METRIC_URI));

    FormData fdlReturn = new FormData();
    fdlReturn.left = new FormAttachment(0, 0);
    fdlReturn.top = new FormAttachment(gQuery, margin);
    wlFields.setLayoutData(fdlReturn);

    int fieldWidgetCols = 5;
    int fieldWidgetRows =
        (getInput().getGoogleAnalyticsFields() != null
            ? getInput().getGoogleAnalyticsFields().size()
            : 1);

    ColumnInfo[] ciKeys = new ColumnInfo[fieldWidgetCols];
    ciKeys[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "GoogleAnalyticsDialog.ColumnInfo.FeedFieldType"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {
              GoogleAnalyticsMeta.FIELD_TYPE_DIMENSION,
              GoogleAnalyticsMeta.FIELD_TYPE_METRIC,
              GoogleAnalyticsMeta.FIELD_TYPE_DATA_SOURCE_PROPERTY,
              GoogleAnalyticsMeta.FIELD_TYPE_DATA_SOURCE_FIELD
            },
            true);
    ciKeys[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "GoogleAnalyticsDialog.ColumnInfo.FeedField"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false,
            false);
    ciKeys[1].setUsingVariables(true);
    ciKeys[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "GoogleAnalyticsDialog.ColumnInfo.RenameTo"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false,
            false);
    ciKeys[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "GoogleAnalyticsDialog.ColumnInfo.Type"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            ValueMetaBase.getTypes());
    ciKeys[4] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "GoogleAnalyticsDialog.ColumnInfo.Format"),
            ColumnInfo.COLUMN_TYPE_FORMAT,
            4);

    setTableView(
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciKeys,
            fieldWidgetRows,
            lsMod,
            props));

    FormData fdReturn = new FormData();
    fdReturn.left = new FormAttachment(0, 0);
    fdReturn.top = new FormAttachment(wlFields, margin);
    fdReturn.right = new FormAttachment(100, 0);
    fdReturn.bottom = new FormAttachment(wLimit, -margin);
    getTableView().setLayoutData(fdReturn);

    fileChooser.addListener(SWT.Selection, this::browseKeyFile);

    /*************************************************
     * // POPULATE AND OPEN DIALOG
     *************************************************/

    getData();

    getInput().setChanged(backupChanged);
    shell.setTabList(new Control[] {wTransformName, gConnect, gQuery, getTableView()});
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void browseKeyFile(Event e) {
    BaseDialog.presentFileDialog(
        shell,
        keyFilename,
        variables,
        new String[] {"*.properties", "*.*"},
        new String[] {"Properties files (*.properties)", "All Files (*.*)"},
        true);
  }

  private RunReportResponse getReportResponse() {
    BetaAnalyticsDataClient analyticsData = null;
    try {
      InputStream inputStream = new FileInputStream(keyFilename.getText());
      Credentials credentials = ServiceAccountCredentials.fromStream(inputStream);

      BetaAnalyticsDataSettings settings =
          BetaAnalyticsDataSettings.newHttpJsonBuilder()
              .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
              .build();
      analyticsData = BetaAnalyticsDataClient.create(settings);
    } catch (IOException e) {
      new ErrorDialog(
          shell,
          "Error creating connection",
          "Error reading key file or creating Google Analytics connection",
          e);
    }

    List<Dimension> dimensionList = new ArrayList<>();
    String dimensionString = wQuDimensions.getText();
    for (String dimension : dimensionString.split(",")) {
      dimensionList.add(Dimension.newBuilder().setName(dimension).build());
    }
    List<Metric> metricList = new ArrayList<>();
    String metricsString = wQuMetrics.getText();
    for (String metric : metricsString.split(",")) {
      metricList.add(Metric.newBuilder().setName(metric).build());
    }
    RunReportRequest request =
        RunReportRequest.newBuilder()
            .setProperty("properties/" + wGaPropertyId.getText())
            .addAllDimensions(dimensionList)
            .addAllMetrics(metricList)
            .addDateRanges(
                DateRange.newBuilder()
                    .setStartDate(wQuStartDate.getText())
                    .setEndDate(wQuEndDate.getText()))
            .build();
    return analyticsData.runReport(request);
  }

  // Visible for testing
  void getFields() {

    getTableView().removeAll();

    RunReportResponse response = getReportResponse();
    List<DimensionHeader> dimensionHeaders = response.getDimensionHeadersList();
    List<MetricHeader> metricHeaders = response.getMetricHeadersList();

    if (response == null || dimensionHeaders.isEmpty() || metricHeaders.isEmpty()) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText("Query yields empty feed");
      mb.setMessage("The feed did not give any results. Please specify a query that returns data.");
      mb.open();

      return;
    }

    int i = 0;
    getTableView().table.setItemCount(dimensionHeaders.size() + metricHeaders.size());
    for (DimensionHeader colHeader : dimensionHeaders) {
      String name = colHeader.getName();
      TableItem item = getTableView().table.getItem(i);
      item.setText(1, GoogleAnalyticsMeta.FIELD_TYPE_DIMENSION);
      item.setText(2, name);
      item.setText(3, name);
      item.setText(4, ValueMetaBase.getTypeDesc(IValueMeta.TYPE_STRING));
      item.setText(5, "");
      i++;
    }
    for (MetricHeader metricHeader : metricHeaders) {
      TableItem item = getTableView().table.getItem(i);
      String name = metricHeader.getName();
      item.setText(1, GoogleAnalyticsMeta.FIELD_TYPE_METRIC);
      item.setText(2, name);
      item.setText(3, name);
      MetricType metricType = metricHeader.getType();
      if (metricType.equals(MetricType.TYPE_INTEGER)) {
        item.setText(4, ValueMetaBase.getTypeDesc(IValueMeta.TYPE_INTEGER));
        item.setText(5, "#;-#");
      } else if (metricType.equals(MetricType.TYPE_FLOAT)
          || metricType.equals(MetricType.TYPE_SECONDS)
          || metricType.equals(MetricType.TYPE_MILLISECONDS)
          || metricType.equals(MetricType.TYPE_MINUTES)
          || metricType.equals(MetricType.TYPE_HOURS)
          || metricType.equals(MetricType.TYPE_STANDARD)
          || metricType.equals(MetricType.TYPE_CURRENCY)
          || metricType.equals(MetricType.TYPE_FEET)
          || metricType.equals(MetricType.TYPE_MILES)
          || metricType.equals(MetricType.TYPE_METERS)
          || metricType.equals(MetricType.TYPE_KILOMETERS)) {
        item.setText(4, ValueMetaBase.getTypeDesc(IValueMeta.TYPE_NUMBER));
        item.setText(5, "#.#;-#.#");
      } else {
        item.setText(4, ValueMetaBase.getTypeDesc(IValueMeta.TYPE_STRING));
        item.setText(5, "");
      }
      i++;
    }

    getTableView().removeEmptyRows();
    getTableView().setRowNums();
    getTableView().optWidth(true);
    getInput().setChanged();
  }

  private void getInfo(GoogleAnalyticsMeta meta) {

    transformName = wTransformName.getText(); // return value

    meta.setGaAppName(wGaAppName.getText());
    meta.setOAuthServiceAccount(wOauthAccount.getText());
    meta.setOAuthKeyFile(keyFilename.getText());
    meta.setGaProperty(wGaPropertyId.getText());

    meta.setStartDate(wQuStartDate.getText());
    meta.setEndDate(wQuEndDate.getText());

    meta.setDimensions(wQuDimensions.getText());
    meta.setMetrics(wQuMetrics.getText());
    meta.setSort(wQuSort.getText());

    int nrFields = getTableView().nrNonEmpty();

    List<GoogleAnalyticsField> googleAnalyticsFields = new ArrayList<>();
    for (int i = 0; i < nrFields; i++) {
      TableItem item = getTableView().getNonEmpty(i);
      GoogleAnalyticsField field = new GoogleAnalyticsField();
      field.setFeedFieldType(item.getText(1));
      field.setFeedField(item.getText(2));
      field.setOutputFieldName(item.getText(3));
      field.setType(item.getText(4));
      field.setInputFormat(item.getText(5));
      googleAnalyticsFields.add(field);
    }
    meta.setGoogleAnalyticsFields(googleAnalyticsFields);
    meta.setRowLimit(Const.toInt(wLimit.getText(), 0));
  }

  // Preview the data
  private void preview() {
    // Create the XML input transform
    GoogleAnalyticsMeta oneMeta = new GoogleAnalyticsMeta();
    getInfo(oneMeta);

    EnterNumberDialog numberDialog =
        new EnterNumberDialog(
            shell,
            props.getDefaultPreviewSize(),
            BaseMessages.getString(PKG, "GoogleAnalyticsDialog.PreviewSize.DialogTitle"),
            BaseMessages.getString(PKG, "GoogleAnalyticsDialog.PreviewSize.DialogMessage"));
    int previewSize = numberDialog.open();
    if (previewSize > 0) {
      oneMeta.setRowLimit(previewSize);
      PipelineMeta previewMeta =
          PipelinePreviewFactory.generatePreviewPipeline(
              pipelineMeta.getMetadataProvider(), oneMeta, wTransformName.getText());

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

      if (!progressDialog.isCancelled()
          && pipeline.getResult() != null
          && pipeline.getResult().getNrErrors() > 0) {
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

  /** Collect data from the meta and place it in the dialog */
  public void getData() {

    if (getInput().getGaAppName() != null) {
      wGaAppName.setText(getInput().getGaAppName());
    }

    wOauthAccount.setText(Const.NVL(getInput().getOAuthServiceAccount(), ""));
    keyFilename.setText(Const.NVL(getInput().getOAuthKeyFile(), ""));
    wGaPropertyId.setText(Const.NVL(getInput().getGaProperty(), ""));

    if (getInput().getStartDate() != null) {
      wQuStartDate.setText(getInput().getStartDate());
    }

    if (getInput().getEndDate() != null) {
      wQuEndDate.setText(getInput().getEndDate());
    }

    if (getInput().getDimensions() != null) {
      wQuDimensions.setText(getInput().getDimensions());
    }

    if (getInput().getMetrics() != null) {
      wQuMetrics.setText(getInput().getMetrics());
    }

    if (getInput().getSort() != null) {
      wQuSort.setText(getInput().getSort());
    }

    wFields.removeAll();
    wFields.removeEmptyRows();
    if (!input.getGoogleAnalyticsFields().isEmpty()) {

      wFields.table.setItemCount(input.getGoogleAnalyticsFields().size());
      List<GoogleAnalyticsField> googleAnalyticsFields = input.getGoogleAnalyticsFields();

      int i = 0;
      for (GoogleAnalyticsField field : googleAnalyticsFields) {

        TableItem item = wFields.table.getItem(i);

        if (!Utils.isEmpty(field.getType())) {
          item.setText(1, field.getFeedFieldType());
        }

        if (!Utils.isEmpty(field.getFeedField())) {
          item.setText(2, field.getFeedField());
        }

        if (!Utils.isEmpty(field.getOutputFieldName())) {
          item.setText(3, field.getOutputFieldName());
        }

        if (!Utils.isEmpty(field.getType())) {
          item.setText(4, field.getType());
        }

        if (!Utils.isEmpty(field.getInputFormat())) {
          item.setText(5, field.getType());
        }
        i++;
      }
    }

    wFields.removeEmptyRows();
    getTableView().setRowNums();
    getTableView().optWidth(true);

    wLimit.setText(getInput().getRowLimit() + "");

    setActive();
  }

  private void cancel() {
    transformName = null;
    getInput().setChanged(backupChanged);
    dispose();
  }

  // let the meta know about the entered data
  private void ok() {
    getInfo(getInput());
    dispose();
  }

  private void createOauthServiceCredentialsControls() {
    // OathAccount line
    Label wlOauthAccount = new Label(gConnect, SWT.RIGHT);
    wlOauthAccount.setText(BaseMessages.getString(PKG, "GoogleAnalyticsDialog.OauthAccount.Label"));
    PropsUi.setLook(wlOauthAccount);

    FormData fdlOathAccount = new FormData();
    fdlOathAccount.left = new FormAttachment(0, 0);
    fdlOathAccount.top = new FormAttachment(wGaAppName, margin);
    fdlOathAccount.right = new FormAttachment(middle, -margin);

    wlOauthAccount.setLayoutData(fdlOathAccount);
    wOauthAccount = new TextVar(variables, gConnect, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wOauthAccount.setToolTipText(
        BaseMessages.getString(PKG, "GoogleAnalyticsDialog.OauthAccount.Tooltip"));
    PropsUi.setLook(wOauthAccount);

    wOauthAccount.addModifyListener(lsMod);
    FormData fdOathAccount = new FormData();
    fdOathAccount.left = new FormAttachment(middle, 0);
    fdOathAccount.top = new FormAttachment(wGaAppName, margin);
    fdOathAccount.right = new FormAttachment(100, -margin);
    wOauthAccount.setLayoutData(fdOathAccount);

    fileChooser = new Button(gConnect, SWT.PUSH | SWT.CENTER);
    fileChooser.setText(BaseMessages.getString(PKG, ("System.Button.Browse")));
    PropsUi.setLook(fileChooser);

    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(wOauthAccount, margin);
    fileChooser.setLayoutData(fdbFilename);

    Label wlFilename = new Label(gConnect, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, ("GoogleAnalyticsDialog.KeyFile.Label")));
    PropsUi.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.top = new FormAttachment(wOauthAccount, margin);
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);

    keyFilename = new TextVar(variables, gConnect, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    keyFilename.setToolTipText(
        BaseMessages.getString(PKG, "GoogleAnalyticsDialog.KeyFilename.Tooltip"));
    keyFilename.addModifyListener(lsMod);
    PropsUi.setLook(keyFilename);

    FormData fdFilename = new FormData();
    fdFilename.top = new FormAttachment(wOauthAccount, margin);
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.right = new FormAttachment(fileChooser, -margin);
    keyFilename.setLayoutData(fdFilename);

    Label wlGaPropertyId = new Label(gConnect, SWT.RIGHT);
    wlGaPropertyId.setText(BaseMessages.getString(PKG, "GoogleAnalyticsDialog.PropertyId.Label"));
    PropsUi.setLook(wlGaPropertyId);
    FormData fdlGaPropertyId = new FormData();
    fdlGaPropertyId.top = new FormAttachment(keyFilename, margin);
    fdlGaPropertyId.left = new FormAttachment(0, 0);
    fdlGaPropertyId.right = new FormAttachment(middle, -margin);
    wlGaPropertyId.setLayoutData(fdlGaPropertyId);

    wGaPropertyId = new TextVar(variables, gConnect, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wGaPropertyId.setToolTipText(
        BaseMessages.getString(PKG, "GoogleAnalyticsDialog.PropertyId.Tooltip"));
    wGaPropertyId.addModifyListener(lsMod);
    PropsUi.setLook(wGaPropertyId);
    FormData fdGaPropertyId = new FormData();
    fdGaPropertyId.top = new FormAttachment(keyFilename, margin);
    fdGaPropertyId.left = new FormAttachment(middle, 0);
    fdGaPropertyId.right = new FormAttachment(100, -margin);
    wGaPropertyId.setLayoutData(fdGaPropertyId);
  }

  TableView getTableView() {
    return wFields;
  }

  void setTableView(TableView wFields) {
    this.wFields = wFields;
  }

  GoogleAnalyticsMeta getInput() {
    return input;
  }

  void setInput(GoogleAnalyticsMeta input) {
    this.input = input;
  }
}
