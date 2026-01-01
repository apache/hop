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

package org.apache.hop.pipeline.transforms.aws.sns;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class SnsNotify extends BaseTransform<SnsNotifyMeta, SnsNotifyData> {

  public SnsNotify(
      TransformMeta transformMeta,
      SnsNotifyMeta meta,
      SnsNotifyData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    data.aws_sns = new AwsSns(meta, getPipelineMeta(), this);
    if (!data.aws_sns.getAWSConnection()) {
      setErrors(1);
      stopAll();
      setOutputDone();
      return false;
    }

    data.realMessageIDField = resolve(meta.getTFldMessageID());

    return super.init();
  }

  /** This methods gets the Input-Fields indices (if defined) or set them to -1 */
  private void setFieldIndices() {
    // topicARN

    IRowMeta inputMeta = (IRowMeta) getInputRowMeta();

    if (meta.getCInputtopicArn().equals("Y")) {
      data.indexOfFieldtopARN = inputMeta.indexOfValue(meta.getTFldtopicARN());
    } else {
      data.indexOfFieldtopARN = -1;
    }

    // Subject
    if (meta.getCInputSubject().equals("Y")) {
      data.indexOfFieldSubject = inputMeta.indexOfValue(meta.getTFldSubject());
    } else {
      data.indexOfFieldSubject = -1;
    }

    // Message
    if (meta.getCInputMessage().equals("Y")) {
      data.indexOfFieldMessage = inputMeta.indexOfValue(meta.getTFldMessage());
    } else {
      data.indexOfFieldMessage = -1;
    }
  }

  /**
   * Once the pipeline starts executing, the processRow() method is called repeatedly by Apache Hop
   * for as long as it returns true. To indicate that a transform has finished processing rows this
   * method must call setOutputDone() and return false;
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

    // get incoming row, getRow() potentially blocks waiting for more rows, returns null if no more
    // rows expected
    Object[] r = getRow();

    // if no more rows are expected, indicate transform is finished and processRow() should not be
    // called again
    if (r == null) {
      setOutputDone();
      return false;
    }

    // Get Field Indices of InputRow
    setFieldIndices();

    // the "first" flag is inherited from the base transform implementation
    // it is used to guard some processing tasks, like figuring out field indexes
    // in the row structure that only need to be done once
    boolean firstrow = false;

    if (first) {
      firstrow = true;
      first = false;
      // clone the input row structure and place it in our data object
      data.outputRowMeta = (IRowMeta) getInputRowMeta().clone();
      // use meta.getFields() to change it, so it reflects the output row structure
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
    }

    // Check if it is first row or notification is set for each row
    if (meta.getNotifyPointShort().equals("each") || firstrow) {

      try {

        Object[] outputRow = sendSnsNotification(r);
        if (outputRow != null) {
          putRow(data.outputRowMeta, outputRow);
          incrementLinesOutput();
          incrementLinesWritten();
        }

      } catch (Exception e) {

        if (getTransformMeta().isDoingErrorHandling()) {
          putError(getInputRowMeta(), r, 1L, e.getMessage(), "", "SNSNotifyError");

        } else {
          logError("AWS SNS Error: " + e.getMessage());
          setErrors(1);
          stopAll();
          setOutputDone(); // signal end to receiver(s)
          return false;
        }
      }

    } else {
      putRow(data.outputRowMeta, r);
      incrementLinesWritten();
    }

    // log progress if it is time to do so
    if (checkFeedback(getLinesRead()) && isBasic()) {
      logBasic("Linenr " + getLinesRead()); // Some basic logging
    }

    // indicate that processRow() should be called again
    return true;
  }

  /**
   * Prepare SNS Notification, send it and store MessageID
   *
   * @param row Current processed row Object
   * @return Modified row Object or null on error
   * @throws Exception
   */
  private Object[] sendSnsNotification(Object[] row) throws Exception {

    try {

      // Notification Content from fields or static input
      String tARN = "";
      String subject = "";
      String message = "";

      // topicARN
      if (data.indexOfFieldtopARN >= 0) {
        tARN = getInputRowMeta().getString(row, data.indexOfFieldtopARN);
      } else {
        tARN = resolve(meta.getTValuetopicARN());
      }
      // Subject
      if (data.indexOfFieldSubject >= 0) {
        subject = getInputRowMeta().getString(row, data.indexOfFieldSubject);
      } else {
        subject = resolve(meta.getTValueSubject());
      }
      // Message
      if (data.indexOfFieldMessage >= 0) {
        message = getInputRowMeta().getString(row, data.indexOfFieldMessage);
      } else {
        message = resolve(meta.getTValueMessage());
      }

      // Send notification and catch messageID
      Object[] outputRowData = row;

      String messageID = data.aws_sns.publishToSNS(tARN, subject, message);

      if (messageID != null) {

        outputRowData = RowDataUtil.resizeArray(outputRowData, data.outputRowMeta.size());

        int indexOfMessID = data.outputRowMeta.indexOfValue(data.realMessageIDField);
        if (indexOfMessID >= 0) {
          outputRowData[indexOfMessID] = messageID;
        }
      }

      return outputRowData;

    } catch (Exception e) {

      // logError(e.getMessage());
      throw e;
    }
  }

  /**
   * This method is called by Apache Hop once the transform is done processing.
   *
   * <p>The dispose() method is the counterpart to init() and should release any resources acquired
   * for transform execution like file handles or database connections.
   */
  public void dispose() {

    data.aws_sns.disconnectAWSConnection();

    super.dispose();
  }
}
