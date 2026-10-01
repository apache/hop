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

import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.io.splunk.SplunkEventCoder;
import org.apache.beam.sdk.io.splunk.SplunkIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.SplunkWriteErrorCoder;
import org.apache.hop.beam.core.fn.HopToSplunkFn;
import org.apache.hop.beam.core.fn.SplunkWriteFailureFn;

/** Terminal HEC sink. Rejected events fail the pipeline and are not Hop output rows. */
@RequiredArgsConstructor
public class BeamSplunkOutputTransform extends PTransform<PCollection<HopRow>, PDone> {
  private final String transformName;
  private final String url;
  private final String token;
  private final Integer batchCount;
  private final boolean disableCertificateValidation;
  private final boolean enableGzip;
  private final String rootCaCertificatePath;
  private final String eventField;
  private final String rowMetaXml;
  private final String host;
  private final String source;
  private final String sourceType;
  private final String index;

  public SplunkIO.Write splunkWrite() {
    SplunkIO.Write write =
        SplunkIO.write(url, token)
            .withEnableGzipHttpCompression(enableGzip)
            .withDisableCertificateValidation(disableCertificateValidation);
    if (batchCount != null) write = write.withBatchCount(batchCount);
    if (StringUtils.isNotEmpty(rootCaCertificatePath))
      write = write.withRootCaCertificatePath(rootCaCertificatePath);
    return write;
  }

  @Override
  public PDone expand(PCollection<HopRow> input) {
    input
        .apply(
            "Convert Hop event",
            ParDo.of(
                new HopToSplunkFn(
                    transformName, eventField, rowMetaXml, host, source, sourceType, index)))
        .setCoder(SplunkEventCoder.of())
        .apply("Write Splunk", splunkWrite())
        .setCoder(SplunkWriteErrorCoder.of())
        .apply("Fail rejected Splunk events", ParDo.of(new SplunkWriteFailureFn(transformName)));
    return PDone.in(input.getPipeline());
  }
}
