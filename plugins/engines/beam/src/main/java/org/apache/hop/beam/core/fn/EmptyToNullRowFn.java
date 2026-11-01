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

import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hop.beam.core.HopRow;

/** Emits one all-null row when a Parquet input received no rows. */
@RequiredArgsConstructor
public class EmptyToNullRowFn extends DoFn<Long, HopRow> {
  private final int width;

  @ProcessElement
  public void process(@Element Long count, OutputReceiver<HopRow> output) {
    if (count != null && count == 0L) {
      output.output(new HopRow(new Object[width]));
    }
  }
}
