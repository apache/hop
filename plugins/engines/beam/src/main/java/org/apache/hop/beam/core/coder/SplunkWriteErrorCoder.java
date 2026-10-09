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

package org.apache.hop.beam.core.coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.splunk.SplunkWriteError;

/**
 * Beam's inferred schema coder drops every field of {@link SplunkWriteError}. The class is an
 * {@code @AutoValue} with {@code @DefaultSchema(AutoValueSchema)}, and that schema only keeps
 * methods whose names start with {@code get} or {@code is}. {@code statusCode()}, {@code
 * statusMessage()} and {@code payload()} therefore round-trip as null.
 */
public class SplunkWriteErrorCoder extends AtomicCoder<SplunkWriteError> {
  private static final SplunkWriteErrorCoder INSTANCE = new SplunkWriteErrorCoder();
  private static final Coder<Integer> STATUS = NullableCoder.of(VarIntCoder.of());
  private static final Coder<String> TEXT = NullableCoder.of(StringUtf8Coder.of());

  public static SplunkWriteErrorCoder of() {
    return INSTANCE;
  }

  private SplunkWriteErrorCoder() {}

  @Override
  public void encode(SplunkWriteError value, OutputStream outStream) throws IOException {
    STATUS.encode(value.statusCode(), outStream);
    TEXT.encode(value.statusMessage(), outStream);
    TEXT.encode(value.payload(), outStream);
  }

  @Override
  public SplunkWriteError decode(InputStream inStream) throws IOException {
    Integer status = STATUS.decode(inStream);
    String statusMessage = TEXT.decode(inStream);
    String payload = TEXT.decode(inStream);
    SplunkWriteError.Builder builder = SplunkWriteError.newBuilder();
    if (status != null) {
      builder.withStatusCode(status);
    }
    if (statusMessage != null) {
      builder.withStatusMessage(statusMessage);
    }
    if (payload != null) {
      builder.withPayload(payload);
    }
    return builder.create();
  }
}
