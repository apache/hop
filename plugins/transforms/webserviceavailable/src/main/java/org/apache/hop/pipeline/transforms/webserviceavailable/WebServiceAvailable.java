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

package org.apache.hop.pipeline.transforms.webserviceavailable;

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.io.CountingInputStream;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.LineageHttpIoEmitter;
import org.apache.hop.lineage.model.HttpDirection;
import org.apache.hop.lineage.model.HttpLineagePayload;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Check if a webservice is available * */
public class WebServiceAvailable
    extends BaseTransform<WebServiceAvailableMeta, WebServiceAvailableData> {
  private static final Class<?> PKG = WebServiceAvailableMeta.class;

  public WebServiceAvailable(
      TransformMeta transformMeta,
      WebServiceAvailableMeta meta,
      WebServiceAvailableData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    // Get row from input rowset & set row busy!
    Object[] r = getRow();
    // no more input to be expected...
    if (r == null) {

      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      // get the RowMeta
      data.previousRowMeta = getInputRowMeta().clone();
      data.nrPrevFields = data.previousRowMeta.size();
      data.outputRowMeta = data.previousRowMeta;
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      // Check is URL field is provided
      if (Utils.isEmpty(meta.getUrlField())) {
        logError(BaseMessages.getString(PKG, "WebServiceAvailable.Error.FilenameFieldMissing"));
        throw new HopException(
            BaseMessages.getString(PKG, "WebServiceAvailable.Error.FilenameFieldMissing"));
      }

      // cache the position of the field
      data.indexOfURL = data.previousRowMeta.indexOfValue(meta.getUrlField());
      if (data.indexOfURL < 0) {
        // The field is unreachable !
        logError(
            BaseMessages.getString(PKG, "WebServiceAvailable.Exception.CouldnotFindField")
                + "["
                + meta.getUrlField()
                + "]");
        throw new HopException(
            BaseMessages.getString(
                PKG, "WebServiceAvailable.Exception.CouldnotFindField", meta.getUrlField()));
      }
    } // End If first

    try {
      // get url
      String url = data.previousRowMeta.getString(r, data.indexOfURL);

      if (Utils.isEmpty(url)) {
        throw new HopException(BaseMessages.getString(PKG, "WebServiceAvailable.Error.URLEmpty"));
      }

      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "WebServiceAvailable.Log.CheckingURL", url));
      }

      boolean webServiceAvailable = false;
      long httpStart = System.currentTimeMillis();
      Integer httpStatus = null;
      Long httpRespBytes = null;
      boolean httpOk = false;
      try {
        URLConnection conn = new URL(url).openConnection();
        conn.setConnectTimeout(data.connectTimeOut);
        conn.setReadTimeout(data.readTimeOut);
        if (conn instanceof HttpURLConnection) {
          httpStatus = ((HttpURLConnection) conn).getResponseCode();
        }
        try (CountingInputStream countingIn = new CountingInputStream(conn.getInputStream())) {
          byte[] buf = new byte[8192];
          while (countingIn.read(buf) != -1) {
            // drain response so byte count reflects transferred body
          }
          httpRespBytes = countingIn.getCount();
        }
        webServiceAvailable = true;
        httpOk = true;
      } catch (Exception e) {
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG, "WebServiceAvailable.Error.ServiceNotReached", url, e.toString()));
        }

      } finally {
        try {
          LineageHttpIoEmitter.emitTransformHttpIo(
              this,
              new HttpLineagePayload(
                  HttpDirection.CLIENT,
                  "GET",
                  url,
                  httpStatus,
                  null,
                  httpRespBytes != null && httpRespBytes > 0 ? httpRespBytes : null,
                  System.currentTimeMillis() - httpStart,
                  httpOk,
                  null));
        } catch (Exception ignored) {
          // optional lineage
        }
      }

      // addwebservice available to the row
      // copy row to output rowset(s)
      putRow(
          data.outputRowMeta, RowDataUtil.addValueData(r, data.nrPrevFields, webServiceAvailable));

      if (isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(
                PKG,
                "FileExists.LineNumber",
                getLinesRead() + " : " + getInputRowMeta().getString(r)));
      }
    } catch (Exception e) {
      boolean sendToErrorRow = false;
      String errorMessage = null;

      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError(
            BaseMessages.getString(PKG, "WebServiceAvailable.ErrorInTransformRunning")
                + e.getMessage());
        setErrors(1);
        stopAll();
        // signal end to receiver(s)
        setOutputDone();
        return false;
      }
      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(
            getInputRowMeta(),
            r,
            1,
            errorMessage,
            meta.getResultFieldName(),
            "WebServiceAvailable001");
      }
    }

    return true;
  }

  @Override
  public boolean init() {

    if (super.init()) {
      if (Utils.isEmpty(meta.getResultFieldName())) {
        logError(BaseMessages.getString(PKG, "WebServiceAvailable.Error.ResultFieldMissing"));
        return false;
      }
      data.connectTimeOut = Const.toInt(resolve(meta.getConnectTimeOut()), 0);
      data.readTimeOut = Const.toInt(resolve(meta.getReadTimeOut()), 0);
      return true;
    }
    return false;
  }
}
