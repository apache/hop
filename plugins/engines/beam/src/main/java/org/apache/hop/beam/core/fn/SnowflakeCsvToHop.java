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
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.Pipeline;

/** CSV rows produced by SnowflakeIO become Hop rows. Counters are conversion, not COPY success. */
@RequiredArgsConstructor
public class SnowflakeCsvToHop implements SnowflakeIO.CsvMapper<HopRow> {
  private final String transformName;
  private final String rowMetaXml;
  private transient IRowMeta rowMeta;
  private transient Counter readCounter;
  private transient Counter outputCounter;
  private transient Counter errorCounter;

  @Override
  public HopRow mapRow(String[] parts) throws Exception {
    init();
    readCounter.inc();
    try {
      if (parts != null && parts.length > rowMeta.size())
        throw new HopException("Snowflake CSV row has more columns than the field list");
      Object[] values = new Object[rowMeta.size()];
      for (int i = 0; i < rowMeta.size(); i++) {
        String text = parts != null && i < parts.length ? parts[i] : null;
        values[i] = SnowflakeValues.fromCsv(rowMeta.getValueMeta(i), text);
      }
      outputCounter.inc();
      return new HopRow(values);
    } catch (HopException e) {
      errorCounter.inc();
      throw e;
    } catch (RuntimeException e) {
      errorCounter.inc();
      throw new HopException("Unable to convert Snowflake CSV row");
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
      throw new HopException("Unable to initialize Snowflake input fields");
    }
  }
}
