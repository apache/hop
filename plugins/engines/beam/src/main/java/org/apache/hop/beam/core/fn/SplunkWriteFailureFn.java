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
import org.apache.beam.sdk.io.splunk.SplunkWriteError;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.pipeline.Pipeline;

/**
 * SplunkIO emits non-transient HEC failures instead of failing the bundle. Hop treats any such
 * error as a pipeline failure. The status message and payload are omitted so event bodies and
 * tokens cannot leak into the error text.
 */
@RequiredArgsConstructor
public class SplunkWriteFailureFn extends DoFn<SplunkWriteError, Void> {
  private final String transformName;

  @ProcessElement
  public void process(@Element SplunkWriteError error) {
    Metrics.counter(Pipeline.METRIC_NAME_ERROR, transformName).inc();
    throw new HopRuntimeException(message(error));
  }

  /** Status only. The status message and payload can contain the rejected event. */
  public static String message(SplunkWriteError error) {
    Integer status = error == null ? null : error.statusCode();
    if (status == null) {
      return "Splunk HEC write failed without an HTTP status";
    }
    return "Splunk HEC rejected an event with HTTP status " + status;
  }
}
