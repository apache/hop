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

package org.apache.hop.core.compress.gzip;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class GzipCompressionLevelTest {

  @Test
  void levelsRunFromOneToNine() {
    GzipCompressionLevel[] levels = GzipCompressionLevel.values();
    assertEquals(9, levels.length);
    for (int i = 0; i < levels.length; i++) {
      assertEquals(i + 1, levels[i].getLevel());
    }
  }

  @Test
  void descriptionsShowTheNumberAndAName() {
    assertEquals("1 - Fastest", GzipCompressionLevel.FASTEST.getDescription());
    assertEquals("6 - Standard (the Java default)", GzipCompressionLevel.STANDARD.getDescription());
    assertEquals("9 - Smallest files", GzipCompressionLevel.SMALLEST.getDescription());
    for (GzipCompressionLevel level : GzipCompressionLevel.values()) {
      assertEquals(
          level,
          IEnumHasCodeAndDescription.lookupDescription(
              GzipCompressionLevel.class, level.getDescription(), null));
      assertEquals(level.name(), level.getCode());
    }
  }

  @Test
  void noStoredLevelMeansTheLevelUsedBefore() {
    assertEquals(GzipCompressionLevel.STANDARD, GzipCompressionLevel.orNotStoredDefault(null));
    assertEquals(6, GzipCompressionLevel.orNotStoredDefault(null).getLevel());
    assertEquals(
        GzipCompressionLevel.SMALLEST,
        GzipCompressionLevel.orNotStoredDefault(GzipCompressionLevel.SMALLEST));
  }

  @ParameterizedTest
  @EnumSource(GzipCompressionLevel.class)
  void compressedDataReadsBack(GzipCompressionLevel level) throws Exception {
    byte[] data = sampleData();
    assertArrayEquals(data, decompress(compress(level, data)));
  }

  @Test
  void theLevelIsApplied() throws Exception {
    byte[] data = sampleData();
    int fastest = compress(GzipCompressionLevel.FASTEST, data).length;
    int smallest = compress(GzipCompressionLevel.SMALLEST, data).length;
    assertTrue(smallest < fastest, "level 9: " + smallest + " bytes, level 1: " + fastest);
  }

  private static byte[] sampleData() {
    StringBuilder text = new StringBuilder();
    for (int i = 0; i < 20_000; i++) {
      text.append("order ").append(i).append(", customer ").append(i % 977).append('\n');
    }
    return text.toString().getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] compress(GzipCompressionLevel level, byte[] data) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (GZIPOutputStream out = level.newOutputStream(bytes)) {
      out.write(data);
    }
    return bytes.toByteArray();
  }

  private static byte[] decompress(byte[] data) throws IOException {
    try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(data))) {
      return in.readAllBytes();
    }
  }
}
