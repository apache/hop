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
 *
 */

package org.apache.hop.pipeline.transforms.dorisbulkloader;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DorisBulkLoader extends BaseTransform<DorisBulkLoaderMeta, DorisBulkLoaderData> {
  private static final Class<?> PKG = DorisBulkLoaderMeta.class; // For Translator

  public DorisBulkLoader(
      TransformMeta transformMeta,
      DorisBulkLoaderMeta meta,
      DorisBulkLoaderData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // Get row from input rowset & set row busy!

    try {
      if (r == null) {
        // no more input to be expected...
        setOutputDone();

        processStreamLoad(null, first);

        return false;
      }

      if (first) {
        data.inputRowMeta = getInputRowMeta();
        data.setIndexOfBodyField(resolve(meta.getDataField()));
      }

      String rowString =
          Const.NVL(data.inputRowMeta.getString(r, data.getIndexOfBodyField()), null);
      if (isDebug()) {
        logDebug(BaseMessages.getString(PKG, "DorisBulkLoader.Log.StreamLoadRowValue", rowString));
      }

      // write row into buffer and call doris stream load http api
      processStreamLoad(rowString, first);

      if (checkFeedback(getLinesRead()) && isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "DorisBulkLoader.Log.LineNumber") + getLinesRead());
      }

      putRow(getInputRowMeta(), r);
      incrementLinesOutput();

      if (first) {
        first = false;
      }

      return true;

    } catch (Exception e) {
      boolean sendToErrorRow = false;
      String errorMessage = null;
      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError(
            BaseMessages.getString(PKG, "DorisBulkLoader.ErrorInTransformRunning")
                + e.getMessage());
        setErrors(1);
        logError(Const.getStackTracker(e));
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }

      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(
            getInputRowMeta(),
            r,
            1,
            errorMessage,
            null,
            BaseMessages.getString(PKG, "DorisBulkLoader.ErrorCode"));
      }
    }
    return true;
  }

  /**
   * process stream load row
   *
   * @param streamLoadRow the row will be load into doris
   * @param first true then current processed row is the first row
   * @throws Exception
   */
  public void processStreamLoad(String streamLoadRow, boolean first)
      throws DorisStreamLoadException {
    try {
      if (streamLoadRow == null) {
        if (!first) {
          // stop to load buffer data into doris by http api
          data.dorisStreamLoad.endWritingIntoBuffer();
          ResponseContent responseContent = data.dorisStreamLoad.executeDorisStreamLoad();
          if (log.isDetailed()) {
            log.logDetailed(
                BaseMessages.getString(
                    PKG, "DorisBulkLoader.Log.StreamLoadResult", responseContent.toString()));
          }
          // close doris http client
          data.dorisStreamLoad.close();
        }

        return;
      }

      if (first) {
        // init doris stream load
        initStreamLoad();

        data.dorisStreamLoad.startWritingIntoBuffer();
      }

      byte[] record = streamLoadRow.getBytes(StandardCharsets.UTF_8);
      // test if could write into stream load buffer
      if (data.dorisStreamLoad.canWrite(record.length)) {
        data.dorisStreamLoad.writeRecord(record);
      } else {
        // stream load current buffer data into doris, and then write data into buffer again
        data.dorisStreamLoad.endWritingIntoBuffer();
        ResponseContent responseContent = data.dorisStreamLoad.executeDorisStreamLoad();
        if (log.isDetailed()) {
          log.logDetailed(
              BaseMessages.getString(
                  PKG, "DorisBulkLoader.Log.StreamLoadResult", responseContent.toString()));
        }
        data.dorisStreamLoad.startWritingIntoBuffer();
        if (data.dorisStreamLoad.canWrite(record.length)) {
          data.dorisStreamLoad.writeRecord(record);
        } else {
          throw new DorisStreamLoadException(
              BaseMessages.getString(PKG, "DorisBulkLoader.Log.ExceedBufferLimit", streamLoadRow));
        }
      }
    } catch (Exception e) {
      if (e instanceof DorisStreamLoadException) {
        throw (DorisStreamLoadException) e;
      } else {
        throw new DorisStreamLoadException(e);
      }
    }
  }

  /**
   * init stream load
   *
   * @throws IOException
   */
  private void initStreamLoad() throws HopException {
    if (log.isDetailed()) {
      log.logDetailed(
          BaseMessages.getString(
              PKG,
              "DorisBulkLoader.Log.StreamLoadParameter",
              XmlMetadataUtil.serializeObjectToXml(meta)));
    }

    StreamLoadProperty streamLoadProperty = new StreamLoadProperty();
    streamLoadProperty.setFeHost(meta.getFeHost());
    streamLoadProperty.setFeHttpPort(meta.getFeHttpPort());
    streamLoadProperty.setDatabaseName(meta.getDatabaseName());
    streamLoadProperty.setTableName(meta.getTableName());
    streamLoadProperty.setLoginUser(meta.getLoginUser());
    Map<String, String> httpHeaders = new HashMap<>();
    httpHeaders.put(LoadConstants.FORMAT_KEY, meta.getFormat());
    httpHeaders.put(LoadConstants.LINE_DELIMITER_KEY, meta.getLineDelimiter());
    httpHeaders.put(LoadConstants.FIELD_DELIMITER_KEY, meta.getColumnDelimiter());
    List<DorisHeader> headers = meta.getHeaders();
    for (int i = 0; i < headers.size(); i++) {
      DorisHeader header = headers.get(i);
      httpHeaders.put(header.getName(), header.getValue());
    }

    streamLoadProperty.setHttpHeaders(httpHeaders);
    streamLoadProperty.setBufferSize(meta.getBufferSize());
    streamLoadProperty.setBufferCount(meta.getBufferCount());
    streamLoadProperty.setLoginPassword(resolve(meta.getLoginPassword()));

    data.dorisStreamLoad = new DorisStreamLoad(streamLoadProperty);
  }
}
