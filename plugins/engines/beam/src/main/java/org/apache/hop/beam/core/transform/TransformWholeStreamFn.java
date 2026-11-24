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

package org.apache.hop.beam.core.transform;

import java.io.Serial;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.exception.HopRuntimeException;

/**
 * Runs a Hop transform on all the rows of its input at once, on one worker: the single element is
 * the list of every row (see {@link AllRowsFn}). The transform sees the end of its input once,
 * after the last row, as on the local engine. For transforms that need the whole stream, such as
 * Blocking transform, Identify last row or Unique rows (HashSet).
 *
 * <p>The transform itself is set up and run by a {@link TransformFn}: only the way rows arrive
 * differs.
 */
public class TransformWholeStreamFn extends DoFn<List<HopRow>, HopRow> {
  @Serial private static final long serialVersionUID = 95700000000000002L;

  private final TransformFn transformFn;

  public TransformWholeStreamFn(TransformFn transformFn) {
    this.transformFn = transformFn;
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    try {
      transformFn.processWholeStream(
          context.element(),
          new TransformFn.ProcessInitContext(context),
          (tupleTag, row) -> context.output(tupleTag, row));
    } catch (Exception e) {
      throw new HopRuntimeException(
          "Error executing transform '" + transformFn.getTransformName() + "' on all its rows", e);
    }
  }

  @Teardown
  public void tearDown() {
    transformFn.tearDown();
  }
}
