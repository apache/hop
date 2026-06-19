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
package org.apache.hop.pipeline.transforms.googlesheets;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.Permission;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.AddSheetRequest;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetRequest;
import com.google.api.services.sheets.v4.model.ClearValuesRequest;
import com.google.api.services.sheets.v4.model.DeleteSheetRequest;
import com.google.api.services.sheets.v4.model.Request;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.SheetProperties;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.SpreadsheetProperties;
import com.google.api.services.sheets.v4.model.UpdateSheetPropertiesRequest;
import com.google.api.services.sheets.v4.model.ValueRange;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class GoogleSheetsOutput
    extends BaseTransform<GoogleSheetsOutputMeta, GoogleSheetsOutputData> {

  private String spreadsheetID;
  private NetHttpTransport httpTransport;

  @FunctionalInterface
  private interface RequestExecutor<T> {
    T execute() throws Exception;
  }

  public GoogleSheetsOutput(
      TransformMeta transformMeta,
      GoogleSheetsOutputMeta meta,
      GoogleSheetsOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  /** Initialize and do work where other transforms need to wait for... */
  @Override
  public boolean init() {
    JsonFactory jsonFactory;
    NetHttpTransport httpTransport;
    String scope;
    boolean exists = false;

    if (super.init()) {

      // Check if file exists
      try {
        httpTransport =
            GoogleSheetsConnectionFactory.newTransport(
                resolve(meta.getProxyHost()), resolve(meta.getProxyPort()));
        jsonFactory = JacksonFactory.getDefaultInstance();
        scope = "https://www.googleapis.com/auth/drive";

        HttpRequestInitializer credential =
            GoogleSheetsCredentials.getCredentialsJson(
                scope,
                resolve(meta.getJsonCredentialPath()),
                resolve(meta.getImpersonation()),
                variables,
                httpTransport);
        Drive service =
            new Drive.Builder(
                    httpTransport,
                    jsonFactory,
                    GoogleSheetsCredentials.setHttpTimeout(credential, resolve(meta.getTimeout())))
                .setApplicationName(GoogleSheetsCredentials.APPLICATION_NAME)
                .build();
        spreadsheetID = resolve(meta.getSpreadsheetKey());

        service
            .files()
            .get(spreadsheetID)
            .setSupportsAllDrives(true)
            .setFields("id, mimeType")
            .execute();
        exists = true;
        if (isBasic()) {
          logBasic("Spreadsheet:" + spreadsheetID + " exists");
        }

        boolean worksheetExists = false;
        if (exists) {
          data.service =
              new Sheets.Builder(
                      httpTransport,
                      jsonFactory,
                      GoogleSheetsCredentials.setHttpTimeout(
                          credential, resolve(meta.getTimeout())))
                  .setApplicationName(GoogleSheetsCredentials.APPLICATION_NAME)
                  .build();

          Spreadsheet spreadSheet =
              executeSheetsRequestWithRetry(
                  () ->
                      data.service.spreadsheets().get(resolve(meta.getSpreadsheetKey())).execute(),
                  "loading spreadsheet metadata");
          List<Sheet> sheets = spreadSheet.getSheets();
          for (Sheet sheet : sheets) {
            if (sheet.getProperties().getTitle().equals(resolve(meta.getWorksheetId()))) {
              worksheetExists = true;
              // the sheet exists, but we need to recreate it, so we'll delete it here first
              if (meta.isReplaceSheet()) {
                DeleteSheetRequest deleteSheetRequest =
                    new DeleteSheetRequest().setSheetId(sheet.getProperties().getSheetId());
                Request request = new Request().setDeleteSheet(deleteSheetRequest);
                List<Request> requests = Collections.singletonList(request);
                BatchUpdateSpreadsheetRequest batchUpdateSpreadsheetRequest =
                    new BatchUpdateSpreadsheetRequest().setRequests(requests);
                executeSheetsRequestWithRetry(
                    () ->
                        data.service
                            .spreadsheets()
                            .batchUpdate(spreadsheetID, batchUpdateSpreadsheetRequest)
                            .execute(),
                    "deleting worksheet before replace");
                worksheetExists = false;
                if (isDetailed()) {
                  logDetailed("deleted sheet " + sheet.getProperties().getTitle());
                }
              }
            }
          }

          if (!worksheetExists) {
            List<Request> requests = new ArrayList<>();
            requests.add(
                new Request()
                    .setAddSheet(
                        new AddSheetRequest()
                            .setProperties(
                                new SheetProperties().setTitle(resolve(meta.getWorksheetId())))));
            BatchUpdateSpreadsheetRequest body =
                new BatchUpdateSpreadsheetRequest().setRequests(requests);
            executeSheetsRequestWithRetry(
                () ->
                    data.service
                        .spreadsheets()
                        .batchUpdate(resolve(meta.getSpreadsheetKey()), body)
                        .execute(),
                "creating worksheet");
          }
        }

        // If it does not exist & create checkbox is checker create it.
        if (!exists && meta.isCreate()) {
          if (meta.isAppend()) { // si append + create alors erreur
            // Init Service
            scope = "https://www.googleapis.com/auth/spreadsheets";

            data.service =
                new Sheets.Builder(
                        httpTransport,
                        jsonFactory,
                        GoogleSheetsCredentials.setHttpTimeout(
                            credential, resolve(meta.getTimeout())))
                    .setApplicationName(GoogleSheetsCredentials.APPLICATION_NAME)
                    .build();

            // If it does not exist create it.
            Spreadsheet spreadsheet =
                new Spreadsheet()
                    .setProperties(new SpreadsheetProperties().setTitle(spreadsheetID));
            Sheets.Spreadsheets.Create request = data.service.spreadsheets().create(spreadsheet);
            Spreadsheet response =
                executeSheetsRequestWithRetry(() -> request.execute(), "creating spreadsheet");
            spreadsheetID = response.getSpreadsheetId();
            meta.setSpreadsheetKey(spreadsheetID); //
            // If it does not exist we use the Worksheet ID to rename 'Sheet ID'
            if (!"Sheet1".equals(resolve(meta.getWorksheetId()))) {

              SheetProperties title =
                  new SheetProperties().setSheetId(0).setTitle(resolve(meta.getWorksheetId()));
              // make a request with this properties
              UpdateSheetPropertiesRequest rename =
                  new UpdateSheetPropertiesRequest().setProperties(title);
              // set fields you want to update
              rename.setFields("title");
              if (isBasic()) {
                logBasic("Changing worksheet title to:" + resolve(meta.getWorksheetId()));
              }
              List<Request> requests = new ArrayList<>();
              Request request1 = new Request().setUpdateSheetProperties(rename);
              requests.add(request1);
              BatchUpdateSpreadsheetRequest requestBody = new BatchUpdateSpreadsheetRequest();
              requestBody.setRequests(requests);
              // now you can execute batchUpdate with your sheetsService and SHEET_ID
              executeSheetsRequestWithRetry(
                  () ->
                      data.service.spreadsheets().batchUpdate(spreadsheetID, requestBody).execute(),
                  "renaming default worksheet");
            }
          } else {
            logError("Append and Create options cannot be activated altogether");
            return false;
          }

          // now if share email is not null we share with R/W with the email given
          if ((!Utils.isEmpty(resolve(meta.getShareEmail())))
              || (resolve(meta.getShareDomain()) != null
                  && !resolve(meta.getShareDomain()).isEmpty())) {

            String fileId = spreadsheetID;
            JsonBatchCallback<Permission> callback =
                new JsonBatchCallback<>() {
                  @Override
                  public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders)
                      throws IOException {
                    // Handle error
                    logError("Failed sharing file" + e.getMessage());
                  }

                  @Override
                  public void onSuccess(Permission permission, HttpHeaders responseHeaders)
                      throws IOException {
                    if (isBasic()) {
                      logBasic("Shared successfully : Permission ID: " + permission.getId());
                    }
                  }
                };
            BatchRequest batch = service.batch();
            if (!Utils.isEmpty(resolve(meta.getShareEmail()))) {
              if (isBasic()) {
                logBasic("Sharing sheet with:" + resolve(meta.getShareEmail()));
              }
              Permission userPermission =
                  new Permission()
                      .setType("user")
                      .setRole("writer")
                      .setEmailAddress(resolve(meta.getShareEmail()));
              // Using Google drive service here not spreadsheet data.service
              service
                  .permissions()
                  .create(fileId, userPermission)
                  .setFields("id")
                  .queue(batch, callback);
            }
            if (resolve(meta.getShareDomain()) != null
                && !resolve(meta.getShareDomain()).isEmpty()) {
              if (isBasic()) {
                logBasic("Sharing sheet with domain:" + resolve(meta.getShareDomain()));
              }
              Permission domainPermission =
                  new Permission()
                      .setType("domain")
                      .setRole("reader")
                      .setDomain(resolve(meta.getShareDomain()));
              service
                  .permissions()
                  .create(fileId, domainPermission)
                  .setFields("id")
                  .queue(batch, callback);
            }
            batch.execute();
          }
        }

        if (!exists && !meta.isCreate()) {
          logError(
              "Spreadsheet not found (ID: "
                  + spreadsheetID
                  + "). Verify the spreadsheet key and that the service account has access. "
                  + "If running inside GCP, rate limiting can also cause empty responses.");
          return false;
        }

      } catch (HttpResponseException e) {
        // Sheets/Drive API calls (spreadsheets().get(), batchUpdate, permissions, etc.) can throw
        // 429/503 under resource limits - never report these as "file does not exist"
        logResourceLimitOrApiError(
            e,
            "worksheet: "
                + resolve(meta.getWorksheetId())
                + " in spreadsheet: "
                + resolve(meta.getSpreadsheetKey()));
        return false;
      } catch (Exception e) {
        // Unwrap in case a wrapper (e.g. HopException) has HttpResponseException as cause
        HttpResponseException httpEx = findHttpResponseException(e);
        if (httpEx != null) {
          logResourceLimitOrApiError(
              httpEx,
              "worksheet: "
                  + resolve(meta.getWorksheetId())
                  + " in spreadsheet: "
                  + resolve(meta.getSpreadsheetKey()));
        } else {
          logError(
              "Error: for worksheet : "
                  + resolve(meta.getWorksheetId())
                  + " in spreadsheet :"
                  + resolve(meta.getSpreadsheetKey())
                  + " - "
                  + e.getMessage(),
              e);
        }
        return false;
      }

      return true;
    }
    return false;
  }

  /**
   * Log a clear error for resource limits (429, 500, 503) or other API errors, so they are never
   * mistaken for "file does not exist".
   */
  private void logResourceLimitOrApiError(HttpResponseException e, String context) {
    int statusCode = e.getStatusCode();
    if (statusCode == 429) {
      logError(
          "Google API rate limit exceeded (429 Too Many Requests) for "
              + context
              + ". This often occurs when running inside GCP with low latency. "
              + "Consider adding delays between pipeline runs. Details: "
              + e.getMessage(),
          e);
    } else if (statusCode == 403) {
      logError(
          "Access denied (403) for "
              + context
              + ". Ensure the service account has access. Details: "
              + e.getMessage(),
          e);
    } else if (statusCode == 500) {
      logError(
          "Google API internal server error (500) for "
              + context
              + ". This is a transient Google backend error ('backendError'). "
              + "Try again later. Details: "
              + e.getMessage(),
          e);
    } else if (statusCode == 503) {
      logError(
          "Google API temporarily unavailable (503) for "
              + context
              + ". Try again later. Details: "
              + e.getMessage(),
          e);
    } else if (statusCode == 404) {
      logError(
          "Spreadsheet or worksheet not found (404) for "
              + context
              + ". Verify the spreadsheet key and worksheet name. Details: "
              + e.getMessage(),
          e);
    } else {
      logError(
          "Google API error (HTTP "
              + statusCode
              + ") for "
              + context
              + ". Details: "
              + e.getMessage(),
          e);
    }
  }

  /** Walk the cause chain to find an HttpResponseException (e.g. when wrapped by HopException). */
  private HttpResponseException findHttpResponseException(Throwable t) {
    for (Throwable current = t; current != null; current = current.getCause()) {
      if (current instanceof HttpResponseException) {
        return (HttpResponseException) current;
      }
    }
    return null;
  }

  private boolean isRetryableSheetsStatus(int statusCode) {
    return statusCode == 429 || statusCode == 500 || statusCode == 503;
  }

  private <T> T executeSheetsRequestWithRetry(RequestExecutor<T> executor, String context)
      throws Exception {
    int maxRetries = getRetryAttempts();
    long retryDelayMs = getRetryDelayMs();
    for (int attempt = 0; attempt < maxRetries; attempt++) {
      try {
        return executor.execute();
      } catch (Exception e) {
        HttpResponseException httpEx =
            (e instanceof HttpResponseException)
                ? (HttpResponseException) e
                : findHttpResponseException(e);
        if (httpEx == null) {
          throw e;
        }
        int statusCode = httpEx.getStatusCode();
        if (!isRetryableSheetsStatus(statusCode) || attempt >= maxRetries - 1) {
          throw httpEx;
        }
        if (isBasic()) {
          logBasic(
              "Retrying "
                  + context
                  + " in "
                  + retryDelayMs
                  + "ms after HTTP "
                  + statusCode
                  + " (attempt "
                  + (attempt + 1)
                  + "/"
                  + maxRetries
                  + ")");
        }
        try {
          TimeUnit.MILLISECONDS.sleep(retryDelayMs);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw ie;
        }
        retryDelayMs *= 2;
      }
    }
    throw new HopException("Unexpected retry loop exit for: " + context);
  }

  private int getRetryAttempts() {
    return parsePositiveInt(resolve(meta.getRetryAttempts()), 3, "retry attempts");
  }

  private long getRetryDelayMs() {
    long retryDelaySeconds =
        parsePositiveInt(resolve(meta.getRetryDelayMs()), 2, "retry delay (s)");
    return retryDelaySeconds * 1000L;
  }

  private int parsePositiveInt(String value, int defaultValue, String fieldName) {
    if (Utils.isEmpty(value)) {
      return defaultValue;
    }
    try {
      int parsed = Integer.parseInt(value.trim());
      if (parsed < 1) {
        logBasic(
            "Invalid " + fieldName + " value '" + value + "', falling back to " + defaultValue);
        return defaultValue;
      }
      return parsed;
    } catch (NumberFormatException e) {
      logBasic(
          "Unable to parse "
              + fieldName
              + " value '"
              + value
              + "', falling back to "
              + defaultValue);
      return defaultValue;
    }
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    List<Object> r;

    if (first && row != null) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(
          data.outputRowMeta, getTransformName(), null, getTransformMeta(), this, metadataProvider);
      data.rows = new ArrayList<>();
      if (meta.isAppend()) { // If append is checked we do not write the header
        if (isBasic()) {
          logBasic("Appending lines so skipping the header");
        }
        data.currentRow++;
      } else {
        if (isBasic()) {
          logBasic("Writing header");
        }
        r = new ArrayList<>();
        for (int i = 0; i < data.outputRowMeta.size(); i++) {
          IValueMeta v = data.outputRowMeta.getValueMeta(i);
          r.add(v.getName());
        }
        data.rows.add(r);
        data.currentRow++;
      }
    }
    try {
      // if last row is reached
      if (row == null) {
        if (data.currentRow > 0) {
          ClearValuesRequest requestBody = new ClearValuesRequest();
          String range = resolve(meta.getWorksheetId());
          if (isBasic()) {
            logBasic(
                "Clearing range" + range + " in Spreadsheet :" + resolve(meta.getSpreadsheetKey()));
          }
          // Creating service
          httpTransport =
              GoogleSheetsConnectionFactory.newTransport(meta.getProxyHost(), meta.getProxyPort());
          JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
          String scope = SheetsScopes.SPREADSHEETS;
          HttpRequestInitializer credential =
              GoogleSheetsCredentials.getCredentialsJson(
                  scope,
                  resolve(meta.getJsonCredentialPath()),
                  resolve(meta.getImpersonation()),
                  variables,
                  httpTransport);
          data.service =
              new Sheets.Builder(
                      httpTransport,
                      jsonFactory,
                      GoogleSheetsCredentials.setHttpTimeout(
                          credential, resolve(meta.getTimeout())))
                  .setApplicationName(GoogleSheetsCredentials.APPLICATION_NAME)
                  .build();

          if (!meta.isAppend()) // if Append is not checked we clear the sheet and we write content
          {
            // Clearing existing Sheet
            Sheets.Spreadsheets.Values.Clear request =
                data.service
                    .spreadsheets()
                    .values()
                    .clear(resolve(meta.getSpreadsheetKey()), range, requestBody);
            if (isBasic()) {
              logBasic(
                  "Clearing Sheet:"
                      + range
                      + "in Spreadsheet :"
                      + resolve(meta.getSpreadsheetKey()));
            }
            if (request != null) {
              executeSheetsRequestWithRetry(
                  () -> request.execute(), "clearing worksheet before write");
            } else {
              if (isBasic()) {
                logBasic("Nothing to clear");
              }
            }
            // Writing Sheet
            if (isBasic()) {
              logBasic("Writing to Sheet");
            }
            ValueRange body = new ValueRange().setValues(data.rows);
            String valueInputOption = "USER_ENTERED";
            executeSheetsRequestWithRetry(
                () ->
                    data.service
                        .spreadsheets()
                        .values()
                        .update(resolve(meta.getSpreadsheetKey()), range, body)
                        .setValueInputOption(valueInputOption)
                        .execute(),
                "updating worksheet values");

          } else { // Appending if option is checked

            // How the input data should be interpreted.
            String valueInputOption = "USER_ENTERED"; // TODO: Update placeholder value.

            // How the input data should be inserted.
            String insertDataOption = "INSERT_ROWS"; // TODO: Update placeholder value.

            // TODO: Assign values to desired fields of `requestBody`:
            ValueRange body = new ValueRange().setValues(data.rows);
            if (isBasic()) {
              logBasic(
                  "Appending data :"
                      + range
                      + "in Spreadsheet :"
                      + resolve(meta.getSpreadsheetKey()));
            }
            Sheets.Spreadsheets.Values.Append request =
                data.service
                    .spreadsheets()
                    .values()
                    .append(resolve(meta.getSpreadsheetKey()), range, body);
            request.setValueInputOption(valueInputOption);
            request.setInsertDataOption(insertDataOption);
            executeSheetsRequestWithRetry(() -> request.execute(), "appending worksheet values");
          }
        } else {
          if (isBasic()) {
            logBasic("No data found");
          }
        }
        setOutputDone();
        return false;
      } else {
        r = new ArrayList<>();
        for (int i = 0; i < data.outputRowMeta.size(); i++) {
          int length = row.length;
          if (i < length && row[i] != null) {
            r.add(row[i].toString());
          } else {
            r.add("");
          }
        }

        data.rows.add(r);

        putRow(data.outputRowMeta, row);
      }
    } catch (HttpResponseException e) {
      String context =
          "worksheet: "
              + resolve(meta.getWorksheetId())
              + " in spreadsheet: "
              + resolve(meta.getSpreadsheetKey());
      int statusCode = e.getStatusCode();
      String msg;
      if (statusCode == 429) {
        msg =
            "Google API rate limit exceeded (429) while writing to "
                + context
                + ". This often occurs when running inside GCP. Details: "
                + e.getMessage();
      } else if (statusCode == 503) {
        msg =
            "Google API temporarily unavailable (503) while writing to "
                + context
                + ". Try again later. Details: "
                + e.getMessage();
      } else if (statusCode == 403) {
        msg = "Access denied (403) while writing to " + context + ". Details: " + e.getMessage();
      } else {
        msg =
            "Google API error (HTTP "
                + statusCode
                + ") while writing to "
                + context
                + ": "
                + e.getMessage();
      }
      throw new HopException(msg, e);
    } catch (Exception e) {
      HttpResponseException httpEx = findHttpResponseException(e);
      if (httpEx != null) {
        String context =
            "worksheet: "
                + resolve(meta.getWorksheetId())
                + " in spreadsheet: "
                + resolve(meta.getSpreadsheetKey());
        int statusCode = httpEx.getStatusCode();
        String msg;
        if (statusCode == 429) {
          msg =
              "Google API rate limit exceeded (429) while writing to "
                  + context
                  + ". This often occurs when running inside GCP. Details: "
                  + httpEx.getMessage();
        } else if (statusCode == 503) {
          msg =
              "Google API temporarily unavailable (503) while writing to "
                  + context
                  + ". Try again later. Details: "
                  + httpEx.getMessage();
        } else {
          msg =
              "Google API error (HTTP "
                  + statusCode
                  + ") while writing to "
                  + context
                  + ": "
                  + httpEx.getMessage();
        }
        throw new HopException(msg, httpEx);
      }
      throw new HopException(e.getMessage(), e);
    } finally {
      data.currentRow++;
    }

    return true;
  }
}
