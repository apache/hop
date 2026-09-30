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

package org.apache.hop.beam.core.fn;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;

/**
 * Issue #2275: keys a HopRow on one of its fields so Beam can group it.
 *
 * <p>A {@link SerializableFunction} rather than a DoFn because that is what {@code
 * WithKeys.of(...)} takes. The row passes through unchanged; only the key is added, so no extra
 * column reaches the output.
 */
public class HopKeyFn implements SerializableFunction<HopRow, String> {

  private final String transformName;
  private final String rowMetaJson;
  private final int keyIndex;

  private transient IRowMeta rowMeta;
  private transient IValueMeta keyValueMeta;

  public HopKeyFn(String transformName, String rowMetaJson, int keyIndex) {
    this.transformName = transformName;
    this.rowMetaJson = rowMetaJson;
    this.keyIndex = keyIndex;
  }

  @Override
  public String apply(HopRow row) {
    try {
      if (rowMeta == null) {
        // The Direct runner may call this before any @Setup hook could run.
        BeamHop.init();
        rowMeta = JsonRowMeta.fromJson(rowMetaJson);
        keyValueMeta = rowMeta.getValueMeta(keyIndex);
      }

      Object raw = keyValueMeta.getString(row.getRow()[keyIndex]);
      // A null key would collapse every such row onto Beam's null key, so normalise it to the
      // empty string: rows without a key still window together rather than failing.
      return raw == null ? "" : raw.toString();

    } catch (Exception e) {
      throw new RuntimeException("Error keying rows in " + transformName, e);
    }
  }
}
