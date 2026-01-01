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

package org.apache.hop.pipeline.transforms.aws.sqs;

import com.amazonaws.services.sqs.model.Message;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class SqsReader extends BaseTransform<SqsReaderMeta, SqsReaderData> {

  /**
   * The PKG member is used when looking up internationalized strings. The properties file with
   * localized keys is expected to reside in {the package of the class
   * specified}/messages/messages_{locale}.properties
   */
  private static Class<?> PKG = SqsReaderMeta.class; // for i18n purposes

  public SqsReader(
      TransformMeta transformMeta,
      SqsReaderMeta meta,
      SqsReaderData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {

    data.aws_sqs = new AwsSqsReader(meta, getPipelineMeta(), this);
    if (!data.aws_sqs.getAWSConnection()) {
      setErrors(1);
      stopAll();
      setOutputDone();
      return false;
    }

    data.realSQSQueue = resolve(meta.getSqsQueue());

    data.realMessageIDFieldName = resolve(meta.getTFldMessageID());
    data.realMessageBodyFieldName = resolve(meta.getTFldMessageBody());
    data.realReceiptHandleFieldName = resolve(meta.getTFldReceiptHandle());
    data.realBodyMD5FieldName = resolve(meta.getTFldBodyMD5());
    data.realSNSMessageFieldName = resolve(meta.getTFldSNSMessage());

    try {
      data.realMaxMessages = Integer.valueOf(resolve(meta.getTFldMaxMessages()));
      if (data.realMaxMessages < 0) {
        throw new NumberFormatException("Max Messages value < 0");
      }

    } catch (NumberFormatException e) {

      logError(BaseMessages.getString(PKG, "SQSReader.Log.MaxMessagesNumber.ERROR"));
      setErrors(1);
      stopAll();
      setOutputDone();
      return false;
    }

    return super.init();
  }

  /**
   * Once the pipeline starts executing, the processRow() method is called repeatedly by Hop for as
   * long as it returns true. To indicate that a transform has finished processing rows this method
   * must call setOutputDone() and return false;
   *
   * <p>Transforms which process incoming rows typically call getRow() to read a single row from the
   * input stream, change or add row content, call putRow() to pass the changed row on and return
   * true. If getRow() returns null, no more rows are expected to come in, and the processRow()
   * implementation calls setOutputDone() and returns false to indicate that it is done too.
   *
   * <p>Transforms which generate rows typically construct a new row Object[] using a call to
   * RowDataUtil.allocateRowData(numberOfFields), add row content, and call putRow() to pass the new
   * row on. Above process may happen in a loop to generate multiple rows, at the end of which
   * processRow() would call setOutputDone() and return false;
   *
   * @return true to indicate that the function should be called again, false if the transform is
   *     done
   */
  @Override
  public boolean processRow() throws HopException {

    // the "first" flag is inherited from the base transform implementation
    // it is used to guard some processing tasks, like figuring out field indexes
    // in the row structure that only need to be done once

    if (first) {
      first = false;
      // clone the input row structure and place it in our data object
      data.outputRowMeta = new RowMeta();
      // use meta.getFields() to change it, so it reflects the output row structure
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, variables, null);

      if (isBasic()) {
        logBasic("Start reading from queue");
      }
    }

    if (Utils.isEmpty(data.realMessageIDFieldName)
        || Utils.isEmpty(data.realMessageBodyFieldName)) {
      logError(BaseMessages.getString(PKG, "SQSReader.Log.NoMessageFields.ERROR"));
      throw new HopException(BaseMessages.getString(PKG, "SQSReader.Log.NoMessageFields.ERROR"));
    }

    // Catch Messages from Queue
    if ((data.realMaxMessages == 0) || (getLinesInput() < data.realMaxMessages)) {

      int numMessages =
          (int) ((data.realMaxMessages == 0) ? 10 : (data.realMaxMessages - getLinesInput()));

      List<Message> messages =
          data.aws_sqs.readMessages(data.realSQSQueue, numMessages, getPipeline().isPreview());

      if (!messages.isEmpty()) {

        for (Message m : messages) {

          Object[] outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());

          int idxMessageIdField = data.outputRowMeta.indexOfValue(data.realMessageIDFieldName);
          if (idxMessageIdField >= 0) {
            outputRow[idxMessageIdField] = m.getMessageId();
          }

          int idxMessageBodyField = data.outputRowMeta.indexOfValue(data.realMessageBodyFieldName);
          if (idxMessageBodyField >= 0) {
            outputRow[idxMessageBodyField] = m.getBody();
          }

          int idxReceiptHandleField =
              data.outputRowMeta.indexOfValue(data.realReceiptHandleFieldName);
          if (idxReceiptHandleField >= 0) {
            outputRow[idxReceiptHandleField] = m.getReceiptHandle();
          }

          int idxBodyMD5Field = data.outputRowMeta.indexOfValue(data.realBodyMD5FieldName);
          if (idxBodyMD5Field >= 0) {
            outputRow[idxBodyMD5Field] = m.getMD5OfBody();
          }

          int idxSNSMessageField = data.outputRowMeta.indexOfValue(data.realSNSMessageFieldName);
          if (idxSNSMessageField >= 0) {
            outputRow[idxSNSMessageField] = getSNSMessageContent(m.getBody());
          }

          putRow(data.outputRowMeta, outputRow);
          incrementLinesInput();
        }
      } else {
        setOutputDone();
        if (isBasic()) {
          logBasic("Finished reading from queue");
        }
        return false;
      }
    } else {
      setOutputDone();
      if (isBasic()) {
        logBasic("Finished reading from queue");
      }
      return false;
    }

    // log progress if it is time to to so
    if (checkFeedback(getLinesRead()) && isBasic()) {
      logBasic("Linenr " + getLinesRead()); // Some basic logging
    }

    // indicate that processRow() should be called again
    return true;
  }

  private String getSNSMessageContent(String body) {

    try {
      ObjectMapper mapper = new ObjectMapper();
      JsonFactory factory = new JsonFactory();
      factory.setCodec(mapper);
      JsonParser parser = factory.createParser(body);
      JsonNode jsonNode = parser.readValueAsTree();
      JsonNode statusNode = jsonNode.get("Message");
      return statusNode.textValue();
    } catch (JsonParseException e) {
      logError("Error parsing JSON: " + e.getMessage());
      return "";
    } catch (IOException e) {
      logError("IO Error: " + e.getMessage());
      return "";
    }
  }
}
