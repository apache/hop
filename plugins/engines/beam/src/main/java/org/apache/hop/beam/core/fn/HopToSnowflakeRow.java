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

package org.apache.hop.beam.core.fn;

import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.Pipeline;

/**
 * Hop rows become CSV values for SnowflakeIO. lines-read and lines-output count conversion. A
 * successful COPY is not a Hop callback and is not counted as written.
 */
@RequiredArgsConstructor
public class HopToSnowflakeRow implements SnowflakeIO.UserDataMapper<HopRow> {
  private final String transformName;
  private final String rowMetaXml;
  private transient IRowMeta rowMeta;
  private transient Counter readCounter;
  private transient Counter outputCounter;
  private transient Counter errorCounter;

  @Override
  public Object[] mapRow(HopRow row) {
    try {
      init();
    } catch (HopException e) {
      throw new HopRuntimeException(e.getMessage());
    }
    readCounter.inc();
    try {
      Object[] incoming = row == null ? null : row.getRow();
      Object[] values = new Object[rowMeta.size()];
      for (int i = 0; i < rowMeta.size(); i++) {
        Object value = incoming != null && i < incoming.length ? incoming[i] : null;
        values[i] = SnowflakeValues.toCsv(rowMeta.getValueMeta(i), value);
      }
      outputCounter.inc();
      return values;
    } catch (HopException e) {
      errorCounter.inc();
      throw new HopRuntimeException(e.getMessage());
    } catch (RuntimeException e) {
      errorCounter.inc();
      throw new HopRuntimeException("Unable to convert Snowflake output row");
    }
  }

  private void init() throws HopException {
    if (rowMeta != null) return;
    readCounter = Metrics.counter(Pipeline.METRIC_NAME_READ, transformName);
    outputCounter = Metrics.counter(Pipeline.METRIC_NAME_OUTPUT, transformName);
    errorCounter = Metrics.counter(Pipeline.METRIC_NAME_ERROR, transformName);
    try {
      BeamHop.init();
      rowMeta =
          new RowMeta(
              XmlHandler.getSubNode(XmlHandler.loadXmlString(rowMetaXml), RowMeta.XML_META_TAG));
      Metrics.counter(Pipeline.METRIC_NAME_INIT, transformName).inc();
    } catch (Exception e) {
      errorCounter.inc();
      throw new HopException("Unable to initialize Snowflake output fields");
    }
  }
}
