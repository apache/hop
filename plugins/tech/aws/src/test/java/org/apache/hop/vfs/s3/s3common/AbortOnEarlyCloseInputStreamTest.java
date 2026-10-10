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

package org.apache.hop.vfs.s3.s3common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.vfs.s3.s3common.S3CommonFileObject.AbortOnEarlyCloseInputStream;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * Closing a ranged object stream aborts the request only when it wasn't read to its end: an aborted
 * request loses its connection, a finished one goes back to the pool.
 */
class AbortOnEarlyCloseInputStreamTest {

  private final AtomicBoolean aborted = new AtomicBoolean();

  private ResponseInputStream<GetObjectResponse> response(int length) {
    return new ResponseInputStream<>(
        GetObjectResponse.builder().build(),
        AbortableInputStream.create(
            new ByteArrayInputStream(new byte[length]), () -> aborted.set(true)));
  }

  @Test
  void keepsTheConnectionOfABoundedRangeReadToItsLength() throws Exception {
    try (AbortOnEarlyCloseInputStream in = new AbortOnEarlyCloseInputStream(response(1000), 1000)) {
      // As a read-ahead block is read: exactly its length, without reading on to the end marker
      assertEquals(1000, in.readNBytes(1000).length);
    }
    assertFalse(aborted.get());
  }

  @Test
  void abortsABoundedRangeClosedEarly() throws Exception {
    try (AbortOnEarlyCloseInputStream in = new AbortOnEarlyCloseInputStream(response(1000), 1000)) {
      assertEquals(10, in.readNBytes(10).length);
    }
    assertTrue(aborted.get());
  }

  @Test
  void abortsAnOpenRangeClosedBeforeItsEnd() throws Exception {
    try (AbortOnEarlyCloseInputStream in = new AbortOnEarlyCloseInputStream(response(1000))) {
      assertEquals(999, in.readNBytes(999).length);
    }
    assertTrue(aborted.get());
  }

  @Test
  void keepsTheConnectionOfAnOpenRangeReadToItsEnd() throws Exception {
    try (AbortOnEarlyCloseInputStream in = new AbortOnEarlyCloseInputStream(response(1000))) {
      in.readAllBytes();
    }
    assertFalse(aborted.get());
  }

  /**
   * After an abort the Apache HTTP client reports the body cut short when the stream is closed.
   * That is the abort working, not an error for the reader.
   */
  @Test
  void closesQuietlyAfterAnAbortTheClientReportsAsCutShort() throws Exception {
    ResponseInputStream<GetObjectResponse> cutShort =
        new ResponseInputStream<>(
            GetObjectResponse.builder().build(),
            AbortableInputStream.create(
                new ByteArrayInputStream(new byte[1000]) {
                  @Override
                  public void close() throws java.io.IOException {
                    if (aborted.get()) {
                      throw new java.io.IOException(
                          "Premature end of Content-Length delimited message body");
                    }
                  }
                },
                () -> aborted.set(true)));

    AbortOnEarlyCloseInputStream in = new AbortOnEarlyCloseInputStream(cutShort);
    assertEquals(10, in.readNBytes(10).length);
    in.close();

    assertTrue(aborted.get());
  }
}
