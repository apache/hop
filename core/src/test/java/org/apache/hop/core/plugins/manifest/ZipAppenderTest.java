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
package org.apache.hop.core.plugins.manifest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.compress.archivers.zip.X5455_ExtendedTimestamp;
import org.apache.commons.compress.archivers.zip.Zip64Mode;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ZipAppenderTest {

  private static final String NAME = "plugins/transforms/foo/plugin-manifest.json";
  private static final byte[] JSON = "{\n  \"schema\" : 1\n}".getBytes(StandardCharsets.UTF_8);

  /** Whole seconds, so the DOS time and the extended timestamp hold them exactly. */
  private static final long OLDER = 1_700_000_000_000L;

  private static final long NEWER = 1_793_094_424_000L;

  @TempDir Path tempDir;

  @Test
  void keepsTheExistingBytesAndCopiesTheTimeOfTheNewestFile() throws Exception {
    Path zip = archive("plugin.zip", Zip64Mode.AsNeeded);
    byte[] before = Files.readAllBytes(zip);
    int directoryOffset = endRecord(before).getInt(16);

    ZipAppender.append(zip, NAME, JSON);

    byte[] after = Files.readAllBytes(zip);
    assertArrayEquals(
        Arrays.copyOf(before, directoryOffset),
        Arrays.copyOf(after, directoryOffset),
        "the entries already in the zip keep their bytes");
    ByteBuffer newest = centralRecord(after, "bin/run.sh");
    ByteBuffer added = centralRecord(after, NAME);
    assertEquals(newest.getInt(12), added.getInt(12), "DOS time and date of the newest file");
    assertEquals(newest.getShort(4), added.getShort(4), "made on Unix, like the newest file");
    assertEquals(0100644 << 16, added.getInt(38), "a plain file, not the 0755 of run.sh");

    try (ZipFile zipFile = ZipFile.builder().setPath(zip).get()) {
      ZipArchiveEntry entry = zipFile.getEntry(NAME);
      assertEquals(ZipEntry.STORED, entry.getMethod());
      X5455_ExtendedTimestamp timestamp =
          (X5455_ExtendedTimestamp) entry.getExtraField(X5455_ExtendedTimestamp.HEADER_ID);
      assertEquals(NEWER / 1000, timestamp.getModifyTime().getValue());
      assertArrayEquals(JSON, zipFile.getInputStream(entry).readAllBytes());
    }
    // A streaming reader goes by the local headers: they have to be right too
    List<String> names = new ArrayList<>();
    try (ZipInputStream in = new ZipInputStream(Files.newInputStream(zip))) {
      for (ZipEntry entry = in.getNextEntry(); entry != null; entry = in.getNextEntry()) {
        names.add(entry.getName());
        if (entry.getName().equals(NAME)) {
          assertArrayEquals(JSON, in.readAllBytes());
        }
      }
    }
    assertEquals(List.of("a.txt", "bin/", "bin/run.sh", NAME), names);
  }

  @Test
  void givesTheSameBytesInEveryTimeZone() throws Exception {
    Path original = archive("plugin.zip", Zip64Mode.AsNeeded);
    TimeZone timeZone = TimeZone.getDefault();
    try {
      List<byte[]> results = new ArrayList<>();
      for (String id : List.of("UTC", "Pacific/Chatham", "America/St_Johns")) {
        TimeZone.setDefault(TimeZone.getTimeZone(id));
        Path zip = Files.copy(original, tempDir.resolve(id.replace('/', '-') + ".zip"));
        ZipAppender.append(zip, NAME, JSON);
        results.add(Files.readAllBytes(zip));
      }
      assertArrayEquals(results.get(0), results.get(1));
      assertArrayEquals(results.get(0), results.get(2));
    } finally {
      TimeZone.setDefault(timeZone);
    }
  }

  @Test
  void replacesTheEntryItAddedBefore() throws Exception {
    Path once = archive("once.zip", Zip64Mode.AsNeeded);
    Path twice = Files.copy(once, tempDir.resolve("twice.zip"));

    ZipAppender.append(once, NAME, JSON);
    ZipAppender.append(
        twice, NAME, "a longer, older manifest".repeat(50).getBytes(StandardCharsets.UTF_8));
    ZipAppender.append(twice, NAME, JSON);

    assertArrayEquals(Files.readAllBytes(once), Files.readAllBytes(twice));
  }

  @Test
  void refusesToReplaceAnEntryThatIsNotTheLastOne() throws Exception {
    Path zip = archive("plugin.zip", Zip64Mode.AsNeeded);
    IOException e = assertThrows(IOException.class, () -> ZipAppender.append(zip, "a.txt", JSON));
    assertTrue(e.getMessage().contains("already contains a.txt"), e.getMessage());
  }

  @Test
  void refusesZip64() throws Exception {
    Path zip = archive("zip64.zip", Zip64Mode.Always);
    IOException e = assertThrows(IOException.class, () -> ZipAppender.append(zip, NAME, JSON));
    assertTrue(e.getMessage().contains("Zip64"), e.getMessage());
  }

  /** A zip as the assembly writes one: Unix modes and extended timestamps. */
  private Path archive(String fileName, Zip64Mode zip64) throws IOException {
    Path zip = tempDir.resolve(fileName);
    try (ZipArchiveOutputStream out = new ZipArchiveOutputStream(zip)) {
      out.setUseZip64(zip64);
      add(out, "a.txt", 0100644, OLDER, "a");
      add(out, "bin/", 040755, NEWER + 60_000, null); // newer, but a folder is never copied
      add(out, "bin/run.sh", 0100755, NEWER, "#!/bin/sh");
    }
    return zip;
  }

  private static void add(
      ZipArchiveOutputStream out, String name, int mode, long time, String content)
      throws IOException {
    ZipArchiveEntry entry = new ZipArchiveEntry(name);
    entry.setUnixMode(mode);
    entry.setTime(time);
    X5455_ExtendedTimestamp timestamp = new X5455_ExtendedTimestamp();
    timestamp.setModifyJavaTime(new Date(time));
    entry.addExtraField(timestamp);
    out.putArchiveEntry(entry);
    if (content != null) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    }
    out.closeArchiveEntry();
  }

  /** The end of central directory record of a zip without a comment. */
  private static ByteBuffer endRecord(byte[] zip) {
    return ByteBuffer.wrap(zip, zip.length - 22, 22).slice().order(ByteOrder.LITTLE_ENDIAN);
  }

  private static ByteBuffer centralRecord(byte[] zip, String name) {
    ByteBuffer end = endRecord(zip);
    int pos = end.getInt(16);
    for (int i = 0; i < (end.getShort(10) & 0xFFFF); i++) {
      ByteBuffer rec = ByteBuffer.wrap(zip, pos, zip.length - pos).slice();
      rec.order(ByteOrder.LITTLE_ENDIAN);
      int nameLength = rec.getShort(28) & 0xFFFF;
      String recName = new String(zip, pos + 46, nameLength, StandardCharsets.UTF_8);
      if (recName.equals(name)) {
        return rec;
      }
      pos += 46 + nameLength + (rec.getShort(30) & 0xFFFF) + (rec.getShort(32) & 0xFFFF);
    }
    throw new AssertionError("no central directory record for " + name);
  }
}
