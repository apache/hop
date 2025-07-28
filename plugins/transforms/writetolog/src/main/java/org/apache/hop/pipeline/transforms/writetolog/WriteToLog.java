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

package org.apache.hop.pipeline.transforms.writetolog;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Write data to log. */
public class WriteToLog extends BaseTransform<WriteToLogMeta, WriteToLogData> {

  private static final Class<?> PKG = WriteToLogMeta.class;

  private int rowCounter = 0;
  private boolean rowCounterLimitHit = false;

  public WriteToLog(
      TransformMeta transformMeta,
      WriteToLogMeta meta,
      WriteToLogData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] row = getRow(); // get row, set busy!
    if (row == null) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    // Limit hit? skip
    if (rowCounterLimitHit) {
      putRow(getInputRowMeta(), row); // copy row to output
      return true;
    }

    if (first) {
      first = false;

      if (!Utils.isEmpty(meta.getLogFields())) {
        data.fieldnrs = new int[meta.getLogFields().size()];
        for (int i = 0; i < data.fieldnrs.length; i++) {
          LogField field = meta.getLogFields().get(i);
          data.fieldnrs[i] = getInputRowMeta().indexOfValue(field.getName());
          if (data.fieldnrs[i] < 0) {
            logError(
                BaseMessages.getString(PKG, "WriteToLog.Log.CanNotFindField", field.getName()));
            throw new HopException(
                BaseMessages.getString(PKG, "WriteToLog.Log.CanNotFindField", field.getName()));
          }
        }
      } else {
        data.fieldnrs = new int[getInputRowMeta().size()];
        for (int i = 0; i < data.fieldnrs.length; i++) {
          data.fieldnrs[i] = i;
        }
      }
      data.fieldnr = data.fieldnrs.length;
      data.logLevel = (meta.getLogLevel() == null) ? LogLevel.BASIC : meta.getLogLevel();
      data.logMessage = Const.NVL(this.resolve(meta.getLogMessage()), "");
      if (!Utils.isEmpty(data.logMessage)) {
        data.logMessage += Const.CR + Const.CR;
      }
    } // end if first

    StringBuilder out = new StringBuilder();
    out.append(
        Const.CR
            + "------------> "
            + BaseMessages.getString(PKG, "WriteToLog.Log.NLigne", "" + getLinesRead())
            + "------------------------------"
            + Const.CR);

    out.append(getRealLogMessage());

    // Loop through fields
    for (int i = 0; i < data.fieldnr; i++) {
      String fieldvalue = getInputRowMeta().getString(row, data.fieldnrs[i]);

      if (meta.isDisplayHeader()) {
        String fieldname = getInputRowMeta().getFieldNames()[data.fieldnrs[i]];
        out.append(fieldname + " = " + fieldvalue + Const.CR);
      } else {
        out.append(fieldvalue + Const.CR);
      }
    }
    out.append(Const.CR + "====================");

    setLog(data.logLevel, out);

    // Increment counter
    if (meta.isLimitRows() && ++rowCounter >= meta.getLimitRowsNumber()) {
      rowCounterLimitHit = true;
    }

    putRow(getInputRowMeta(), row); // copy row to output

    return true;
  }

  /** Output message to log */
  private void setLog(final LogLevel loglevel, final StringBuilder msg) {
    switch (loglevel) {
      case BASIC -> logBasic(msg.toString());
      case ERROR -> logError(msg.toString());
      case MINIMAL -> logMinimal(msg.toString());
      case DETAILED -> logDetailed(msg.toString());
      case DEBUG -> logDebug(msg.toString());
      case ROWLEVEL -> logRowlevel(msg.toString());
      case NOTHING -> {}
    }
  }

  public String getRealLogMessage() {
    return data.logMessage;
  }
}
