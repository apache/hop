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
package org.apache.hop.pipeline.transforms.googlesheet;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.ValueRange;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

/** Describe your transform plugin. */
public class GoogleSheetsPluginInput
    extends BaseTransform<GoogleSheetsPluginInputMeta, GoogleSheetsPluginInputData>
    implements ITransform<GoogleSheetsPluginInputMeta, GoogleSheetsPluginInputData> {

  private static Class<?> PKG =
      GoogleSheetsPluginInput.class; // for i18n purposes, needed by Translator2!!

  public GoogleSheetsPluginInput(
      TransformMeta transformMeta,
      GoogleSheetsPluginInputMeta meta,
      GoogleSheetsPluginInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  /** Initialize and do work where other transforms need to wait for... */
  @Override
  public boolean init() {

    List<TransformMeta> transform = getPipelineMeta().findPreviousTransforms(getTransformMeta());
    data.hasInput = transform != null && transform.size() > 0;

    JsonFactory JSON_FACTORY = null;
    NetHttpTransport HTTP_TRANSPORT = null;
    String APPLICATION_NAME = "hop-google-sheets";
    String scope = SheetsScopes.SPREADSHEETS_READONLY;

    try {
      JSON_FACTORY = JacksonFactory.getDefaultInstance();
      HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
    } catch (Exception e) {
      log.logError("cannot initiate HTTP transport" + e.getMessage());
      return false;
    }

    if (super.init()) {
      try {
        Sheets service =
            new Sheets.Builder(
                    HTTP_TRANSPORT,
                    JSON_FACTORY,
                    GoogleSheetsPluginCredentials.getCredentialsJson(
                        scope, resolve(meta.getJsonCredentialPath())))
                .setApplicationName(APPLICATION_NAME)
                .build();
        String range = resolve(meta.getWorksheetId());
        ValueRange response =
            service.spreadsheets().values().get(resolve(meta.getSpreadsheetKey()), range).execute();
        if (response == null) {
          log.logError(
              "No data found for worksheet : "
                  + resolve(meta.getWorksheetId())
                  + " in spreadsheet :"
                  + resolve(meta.getSpreadsheetKey()));
          return false;
        } else {
          List<List<Object>> values = response.getValues();
          log.logBasic("Reading Sheet, found: " + values.size() + " rows");
          if (values == null || values.isEmpty()) {
            throw new HopTransformException(
                "No response found for worksheet : "
                    + resolve(meta.getWorksheetId())
                    + " in spreadsheet :"
                    + resolve(meta.getSpreadsheetKey()));
          } else {
            data.rows = values;
          }
        }
      } catch (Exception e) {
        log.logError(
            "Error: for worksheet : "
                + resolve(meta.getWorksheetId())
                + " in spreadsheet :"
                + resolve(meta.getSpreadsheetKey())
                + e.getMessage(),
            e);
        return false;
      }

      return true;
    }
    return false;
  }

  public boolean processRow() throws HopException {

    if (first) {
      first = false;
      data.outputRowMeta = new RowMeta();
      meta.getFields(
          data.outputRowMeta, getTransformName(), null, getTransformMeta(), this, metadataProvider);

      data.currentRow++;

    } else {
      try {
        Object[] outputRowData = readRow();
        if (outputRowData == null) {
          setOutputDone();
          return false;
        } else {
          putRow(data.outputRowMeta, outputRowData);
        }
      } catch (Exception e) {
        throw new HopException(e.getMessage());
      } finally {
        data.currentRow++;
      }
    }

    return true;
  }

  private Object getRowDataValue(
      final IValueMeta targetValueMeta,
      final IValueMeta sourceValueMeta,
      final Object value,
      final DateFormat df)
      throws HopException {
    if (value == null) {
      return null;
    }

    if (IValueMeta.TYPE_STRING == targetValueMeta.getType()) {
      return targetValueMeta.convertData(sourceValueMeta, value.toString());
    }

    if (IValueMeta.TYPE_NUMBER == targetValueMeta.getType()) {
      return targetValueMeta.convertData(sourceValueMeta, Double.valueOf(value.toString()));
    }

    if (IValueMeta.TYPE_INTEGER == targetValueMeta.getType()) {
      return targetValueMeta.convertData(sourceValueMeta, Long.valueOf(value.toString()));
    }

    if (IValueMeta.TYPE_BIGNUMBER == targetValueMeta.getType()) {
      return targetValueMeta.convertData(sourceValueMeta, new BigDecimal(value.toString()));
    }

    if (IValueMeta.TYPE_BOOLEAN == targetValueMeta.getType()) {
      return targetValueMeta.convertData(sourceValueMeta, Boolean.valueOf(value.toString()));
    }

    if (IValueMeta.TYPE_BINARY == targetValueMeta.getType()) {
      return targetValueMeta.convertData(sourceValueMeta, value);
    }

    if (IValueMeta.TYPE_DATE == targetValueMeta.getType()) {
      try {
        return targetValueMeta.convertData(sourceValueMeta, df.parse(value.toString()));
      } catch (final ParseException e) {
        throw new HopValueException("Unable to convert data type of value");
      }
    }

    throw new HopValueException("Unable to convert data type of value");
  }

  private Object[] readRow() {
    try {
      logRowlevel("Allocating :" + Integer.toString(data.outputRowMeta.size()));
      Object[] outputRowData = RowDataUtil.allocateRowData(data.outputRowMeta.size());
      int outputIndex = 0;
      logRowlevel(
          "Reading Row: "
              + Integer.toString(data.currentRow)
              + " out of : "
              + Integer.toString(data.rows.size()));
      if (data.currentRow < data.rows.size()) {
        List<Object> row = data.rows.get(data.currentRow);
        for (IValueMeta column : data.outputRowMeta.getValueMetaList()) {
          Object value = null;
          logRowlevel(
              "Reading columns: "
                  + Integer.toString(outputIndex)
                  + " out of : "
                  + Integer.toString(row.size()));
          if (outputIndex > row.size() - 1) {
            logRowlevel("Beyond size");
            outputRowData[outputIndex++] = null;
          } else {
            if (row.get(outputIndex) != null) {
              logRowlevel("getting value" + Integer.toString(outputIndex));
              value = row.get(outputIndex); // .toString();
              logRowlevel("got value " + Integer.toString(outputIndex));
            }
            if (value == null || value.toString().isEmpty()) {
              outputRowData[outputIndex++] = null;
              logRowlevel("null value");
            } else {
              GoogleSheetsPluginInputFields input = meta.getInputFields()[outputIndex];
              DateFormat df =
                  (column.getType() == IValueMeta.TYPE_DATE)
                      ? new SimpleDateFormat(input.getFormat())
                      : null;
              outputRowData[outputIndex++] = getRowDataValue(column, column, value, df);
              // outputRowData[outputIndex++] = value;
              logRowlevel("value : " + value.toString());
            }
          }
        }
      } else {
        log.logBasic(
            "Finished reading last row "
                + Integer.toString(data.currentRow)
                + " / "
                + Integer.toString(data.rows.size()));
        return null;
      }
      return outputRowData;
    } catch (Exception e) {
      log.logError("Exception reading value :" + e.getMessage());
      return null;
    }
  }
}
