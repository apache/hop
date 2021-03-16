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
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.drive.model.Permission;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.AppendValuesResponse;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetRequest;
import com.google.api.services.sheets.v4.model.ClearValuesRequest;
import com.google.api.services.sheets.v4.model.ClearValuesResponse;
import com.google.api.services.sheets.v4.model.Request;
import com.google.api.services.sheets.v4.model.SheetProperties;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.SpreadsheetProperties;
import com.google.api.services.sheets.v4.model.UpdateSheetPropertiesRequest;
import com.google.api.services.sheets.v4.model.UpdateValuesResponse;
import com.google.api.services.sheets.v4.model.ValueRange;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Describe your transform plugin. */
public class GoogleSheetsOutput
    extends BaseTransform<GoogleSheetsOutputMeta, GoogleSheetsOutputData>
    implements ITransform<GoogleSheetsOutputMeta, GoogleSheetsOutputData> {

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
    JsonFactory JSON_FACTORY;
    NetHttpTransport HTTP_TRANSPORT;
    String scope;
    Boolean exists = false;

    if (super.init()) {

      // Check if file exists
      try {
        HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        JSON_FACTORY = JacksonFactory.getDefaultInstance();
        scope = "https://www.googleapis.com/auth/drive";
        Drive service =
            new Drive.Builder(
                    HTTP_TRANSPORT,
                    JSON_FACTORY,
                    GoogleSheetsCredentials.getCredentialsJson(
                        scope, resolve(meta.getJsonCredentialPath())))
                .setApplicationName(GoogleSheetsCredentials.APPLICATION_NAME)
                .build();
        String wsID = resolve(meta.getSpreadsheetKey());
        // "properties has { key='id' and value='"+wsID+"'}";
        String q = "mimeType='application/vnd.google-apps.spreadsheet'";
        FileList result =
            service
                .files()
                .list()
                .setSupportsAllDrives(true)
                .setIncludeItemsFromAllDrives(true)
                .setQ(q)
                .setPageSize(100)
                .setFields("nextPageToken, files(id, name)")
                .execute();
        List<File> spreadsheets = result.getFiles();

        for (File spreadsheet : spreadsheets) {
          // log.logBasic(wsID+" VS "+spreadsheet.getId());
          if (wsID.equals(spreadsheet.getId())) {
            exists = true; // file exists
            log.logBasic("Spreadsheet:" + wsID + " exists");
          }
        }
        // If it does not exist & create checkbox is checker create it.
        // log.logBasic("Create if Not exist is :"+meta.getCreate());
        if (!exists && meta.getCreate()) {
          if (!meta.getAppend()) { // si append + create alors erreur
            // Init Service
            scope = "https://www.googleapis.com/auth/spreadsheets";
            data.service =
                new Sheets.Builder(
                        HTTP_TRANSPORT,
                        JSON_FACTORY,
                        GoogleSheetsCredentials.getCredentialsJson(
                            scope, resolve(meta.getJsonCredentialPath())))
                    .setApplicationName(GoogleSheetsCredentials.APPLICATION_NAME)
                    .build();

            // If it does not exist create it.
            Spreadsheet spreadsheet =
                new Spreadsheet().setProperties(new SpreadsheetProperties().setTitle(wsID));
            Sheets.Spreadsheets.Create request = data.service.spreadsheets().create(spreadsheet);
            Spreadsheet response = request.execute();
            String spreadsheetID = response.getSpreadsheetId();
            meta.setSpreadsheetKey(spreadsheetID); //
            // log.logBasic(response);
            // If it does not exist we use the Worksheet ID to rename 'Sheet ID'
            if (resolve(meta.getWorksheetId()) != "Sheet1") {

              SheetProperties title =
                  new SheetProperties().setSheetId(0).setTitle(resolve(meta.getWorksheetId()));
              // make a request with this properties
              UpdateSheetPropertiesRequest rename =
                  new UpdateSheetPropertiesRequest().setProperties(title);
              // set fields you want to update
              rename.setFields("title");
              log.logBasic("Changing worksheet title to:" + resolve(meta.getWorksheetId()));
              List<Request> requests = new ArrayList<>();
              Request request1 = new Request().setUpdateSheetProperties(rename);
              requests.add(request1);
              BatchUpdateSpreadsheetRequest requestBody = new BatchUpdateSpreadsheetRequest();
              requestBody.setRequests(requests);
              // now you can execute batchUpdate with your sheetsService and SHEET_ID
              data.service.spreadsheets().batchUpdate(spreadsheetID, requestBody).execute();
              // now if share email is not null we share with R/W with the email given
              if ((resolve(meta.getShareEmail()) != null
                      && !resolve(meta.getShareEmail()).isEmpty())
                  || (resolve(meta.getShareDomain()) != null
                      && !resolve(meta.getShareDomain()).isEmpty())) {

                String fileId = spreadsheetID;
                JsonBatchCallback<Permission> callback =
                    new JsonBatchCallback<Permission>() {
                      @Override
                      public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders)
                          throws IOException {
                        // Handle error
                        log.logError("Failed sharing file" + e.getMessage());
                      }

                      @Override
                      public void onSuccess(Permission permission, HttpHeaders responseHeaders)
                          throws IOException {
                        log.logBasic("Shared successfully : Permission ID: " + permission.getId());
                      }
                    };
                BatchRequest batch = service.batch();
                if (resolve(meta.getShareEmail()) != null
                    && !resolve(meta.getShareEmail()).isEmpty()) {
                  log.logBasic("Sharing sheet with:" + resolve(meta.getShareEmail()));
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
                  log.logBasic("Sharing sheet with domain:" + resolve(meta.getShareDomain()));
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
          } else {
            log.logError("Append and Create options cannot be activated altogether");
            return false;
          }
        }

        if (!exists && !meta.getCreate()) {
          log.logError("File does not Exist");
          return false;
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
    Object[] row = getRow();
    List<Object> r;

    if (first && row != null) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(
          data.outputRowMeta, getTransformName(), null, getTransformMeta(), this, metadataProvider);
      data.rows = new ArrayList<>();
      if (meta.getAppend()) { // If append is checked we do not write the header
        logBasic("Appending lines so skipping the header");
        data.currentRow++;
      } else {
        logBasic("Writing header");
        r = new ArrayList<Object>();
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

          logBasic(
              "Clearing range" + range + " in Spreadsheet :" + resolve(meta.getSpreadsheetKey()));
          // Creating service
          NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
          JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
          String scope = SheetsScopes.SPREADSHEETS;
          data.service =
              new Sheets.Builder(
                      HTTP_TRANSPORT,
                      JSON_FACTORY,
                      GoogleSheetsCredentials.getCredentialsJson(
                          scope, resolve(meta.getJsonCredentialPath())))
                  .setApplicationName(GoogleSheetsCredentials.APPLICATION_NAME)
                  .build();

          if (!meta.getAppend()) // if Append is not checked we clear the sheet and we write content
          {
            // Clearing existing Sheet
            Sheets.Spreadsheets.Values.Clear request =
                data.service
                    .spreadsheets()
                    .values()
                    .clear(resolve(meta.getSpreadsheetKey()), range, requestBody);
            logBasic(
                "Clearing Sheet:" + range + "in Spreadsheet :" + resolve(meta.getSpreadsheetKey()));
            if (request != null) {
              ClearValuesResponse response = request.execute();
            } else logBasic("Nothing to clear");
            // Writing Sheet
            logBasic("Writing to Sheet");
            ValueRange body = new ValueRange().setValues(data.rows);
            String valueInputOption = "USER_ENTERED";
            UpdateValuesResponse result =
                data.service
                    .spreadsheets()
                    .values()
                    .update(resolve(meta.getSpreadsheetKey()), range, body)
                    .setValueInputOption(valueInputOption)
                    .execute();

          } else { // Appending if option is checked

            // How the input data should be interpreted.
            String valueInputOption = "USER_ENTERED"; // TODO: Update placeholder value.

            // How the input data should be inserted.
            String insertDataOption = "INSERT_ROWS"; // TODO: Update placeholder value.

            // TODO: Assign values to desired fields of `requestBody`:
            ValueRange body = new ValueRange().setValues(data.rows);
            logBasic(
                "Appending data :"
                    + range
                    + "in Spreadsheet :"
                    + resolve(meta.getSpreadsheetKey()));

            Sheets.Spreadsheets.Values.Append request =
                data.service
                    .spreadsheets()
                    .values()
                    .append(resolve(meta.getSpreadsheetKey()), range, body);
            request.setValueInputOption(valueInputOption);
            request.setInsertDataOption(insertDataOption);
            AppendValuesResponse response = request.execute();
          }
        } else {
          logBasic("No data found");
        }
        setOutputDone();
        return false;
      } else {
        r = new ArrayList<Object>();
        for (int i = 0; i < data.outputRowMeta.size(); i++) {
          int length = row.length;
          // logBasic("Row length:"+length+" VS rowMeta "+data.outputRowMeta.size()+" i="+i);

          if (i < length && row[i] != null) {
            r.add(row[i].toString());
          } else {
            r.add("");
          }
        }

        data.rows.add(r);

        putRow(data.outputRowMeta, row);
      }
    } catch (Exception e) {
      throw new HopException(e.getMessage());
    } finally {
      data.currentRow++;
    }

    return true;
  }
}
