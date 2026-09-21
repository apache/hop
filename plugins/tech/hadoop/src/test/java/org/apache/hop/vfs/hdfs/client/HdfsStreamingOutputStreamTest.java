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
package org.apache.hop.vfs.hdfs.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

class HdfsStreamingOutputStreamTest {

  @Test
  void streamsBytesAndIgnoresFlushAfterClose() throws Exception {
    ByteArrayOutputStream received = new ByteArrayOutputStream();
    var executor = Executors.newSingleThreadExecutor();
    try (HdfsStreamingOutputStream out =
        HdfsStreamingOutputStream.start(
            executor, (in, stream) -> in.transferTo(received), "/it/file.parquet")) {
      out.write("abc".getBytes(StandardCharsets.UTF_8));
      out.flush();
    }
    assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), received.toByteArray());
    // second close / flush after close must not throw (VFS close chains)
    // already closed by try-with-resources; construct another and close twice
    ByteArrayOutputStream received2 = new ByteArrayOutputStream();
    HdfsStreamingOutputStream out2 =
        HdfsStreamingOutputStream.start(
            executor, (in, stream) -> in.transferTo(received2), "/it/x");
    out2.write(1);
    out2.close();
    assertDoesNotThrow(out2::close);
    assertDoesNotThrow(out2::flush);
    executor.shutdownNow();
  }

  @Test
  void closeTimesOutAndUnblocksWhenUploaderNeverReads() throws Exception {
    CountDownLatch block = new CountDownLatch(1);
    var executor = Executors.newSingleThreadExecutor();
    try {
      HdfsStreamingOutputStream out =
          HdfsStreamingOutputStream.start(
              executor,
              (in, stream) -> {
                try {
                  block.await();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new IOException(e);
                }
                in.transferTo(new ByteArrayOutputStream());
              },
              "/it/stuck.parquet",
              1);
      out.write(1);
      long started = System.nanoTime();
      IOException error = assertThrows(IOException.class, out::close);
      assertTrue(error.getMessage().contains("timed out"));
      assertTrue(System.nanoTime() - started < 10_000_000_000L);
    } finally {
      block.countDown();
      executor.shutdownNow();
    }
  }
}
