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

package org.apache.hop.www.service;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import lombok.Getter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.RowAdapter;

/**
 * A web service whose pipeline has been loaded, parameterised and prepared, but not yet started.
 *
 * <p>Splitting resolution from execution lets a transport learn the content type before it commits
 * a response, and means a failure while resolving the service can still be reported with the
 * transport's own error content type rather than the service's.
 */
public class PreparedWebService {

  @Getter private final IPipelineEngine<PipelineMeta> pipeline;
  @Getter private final String serverObjectId;
  @Getter private final String contentType;
  @Getter private final String encoding;
  private final String transformName;
  private final String fieldName;
  private final String statusCodeField;

  PreparedWebService(
      IPipelineEngine<PipelineMeta> pipeline,
      String serverObjectId,
      String contentType,
      String encoding,
      String transformName,
      String fieldName,
      String statusCodeField) {
    this.pipeline = pipeline;
    this.serverObjectId = serverObjectId;
    this.contentType = contentType;
    this.encoding = encoding;
    this.transformName = transformName;
    this.fieldName = fieldName;
    this.statusCodeField = statusCodeField;
  }

  /**
   * Run the pipeline, streaming the configured output field of every row written by the configured
   * transform to the given output.
   *
   * @param output where to write the rows to
   * @throws HopException if the pipeline could not be started or completed
   */
  public void execute(IWebServiceOutput output) throws HopException {
    output.setContentType(contentType, encoding);

    final OutputStream outputStream;
    try {
      outputStream = output.getOutputStream();
    } catch (IOException e) {
      throw new HopException("Unable to open the web service output stream", e);
    }

    // TODO: add to all copies
    IEngineComponent component = pipeline.findComponent(transformName, 0);
    component.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) throws HopTransformException {
            try {
              output.setStatus(rowMeta.getInteger(row, statusCodeField, 200L).intValue());

              // Get the field index and metadata to detect field type
              int fieldIndex = rowMeta.indexOfValue(fieldName);
              if (fieldIndex < 0) {
                throw new HopTransformException("Field '" + fieldName + "' not found in row");
              }

              IValueMeta valueMeta = rowMeta.getValueMeta(fieldIndex);

              // Check if field is binary type and handle accordingly
              byte[] outputData;
              if (valueMeta.getType() == IValueMeta.TYPE_BINARY) {
                // Binary output - get raw bytes without encoding conversion
                outputData = rowMeta.getBinary(row, fieldIndex);
                if (outputData == null) {
                  outputData = new byte[0];
                }
              } else {
                // Text output - convert to string and encode as UTF-8
                String outputString = rowMeta.getString(row, fieldName, "");
                outputData = outputString.getBytes(StandardCharsets.UTF_8);
              }

              outputStream.write(outputData);
              outputStream.flush();
            } catch (HopValueException e) {
              throw new HopTransformException(
                  "Error getting output field '"
                      + fieldName
                      + " from row: "
                      + rowMeta.toStringMeta(),
                  e);
            } catch (IOException e) {
              throw new HopTransformException("Error writing output of '" + fieldName + "'", e);
            }
          }
        });

    pipeline.startThreads();
    pipeline.waitUntilFinished();
  }
}
