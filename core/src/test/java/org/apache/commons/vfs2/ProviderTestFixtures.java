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

package org.apache.commons.vfs2;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;

/**
 * The fixture tree the provider tests read ({@code read-tests}) and write next to ({@code
 * write-tests}). It ships as classpath resources in the hop-core test-jar so any provider module
 * can put it where its tests expect it: a local folder, an embedded server's home, or a bucket.
 */
public final class ProviderTestFixtures {

  /** Classpath folder holding the fixture tree. */
  public static final String RESOURCE_ROOT = "/vfs-provider-test-data/";

  /** Every fixture file, relative to the fixture root. Jars can't be listed, so they are named. */
  public static final List<String> FILES =
      List.of(
          "read-tests/file1.txt",
          "read-tests/empty.txt",
          "read-tests/file%.txt",
          "read-tests/file space.txt",
          "read-tests/dir1/file1.txt",
          "read-tests/dir1/file2.txt",
          "read-tests/dir1/file3.txt",
          "read-tests/dir1/subdir1/file1.txt",
          "read-tests/dir1/subdir1/file2.txt",
          "read-tests/dir1/subdir1/file3.txt",
          "read-tests/dir1/subdir2/file1.txt",
          "read-tests/dir1/subdir2/file2.txt",
          "read-tests/dir1/subdir2/file3.txt",
          "read-tests/dir1/subdir3/file1.txt",
          "read-tests/dir1/subdir3/file2.txt",
          "read-tests/dir1/subdir3/file3.txt",
          "read-tests/dir1/subdir4.jar/file1.txt",
          "read-tests/dir1/subdir4.jar/file2.txt",
          "read-tests/dir1/subdir4.jar/file3.txt",
          "write-tests/dummy.txt");

  /**
   * A generated file, big enough to span the read buffers of every provider, next to (not inside)
   * {@code read-tests} so the folder listings those tests expect stay the same.
   */
  public static final String LARGE_FILE = "large-tests/large.bin";

  /** Size of {@link #LARGE_FILE}: a little over 2 MiB, deliberately not a power of two. */
  public static final int LARGE_SIZE = 2 * 1024 * 1024 + 37;

  private ProviderTestFixtures() {}

  /** The byte at a position of {@link #LARGE_FILE}; no short period, so offsets can't alias. */
  public static byte largeByte(long position) {
    return (byte) ((position * 31) ^ (position >>> 8) ^ (position >>> 16));
  }

  /** The bytes of {@link #LARGE_FILE} from {@code from} (inclusive) to {@code to} (exclusive). */
  public static byte[] largeBytes(long from, long to) {
    byte[] bytes = new byte[(int) (to - from)];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = largeByte(from + i);
    }
    return bytes;
  }

  private static InputStream open(String path) throws IOException {
    InputStream in = ProviderTestFixtures.class.getResourceAsStream(RESOURCE_ROOT + path);
    if (in == null) {
      throw new IOException("Missing provider test fixture " + RESOURCE_ROOT + path);
    }
    return in;
  }

  /** Extracts the fixture tree into a local folder, unless it is already there. */
  public static void extractTo(File folder) throws IOException {
    File large = new File(folder, LARGE_FILE);
    if (new File(folder, "read-tests/file1.txt").isFile() && large.length() == LARGE_SIZE) {
      return;
    }
    for (String path : FILES) {
      File target = new File(folder, path);
      Files.createDirectories(target.getParentFile().toPath());
      try (InputStream in = open(path)) {
        Files.copy(in, target.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
    }
    Files.createDirectories(large.getParentFile().toPath());
    Files.write(large.toPath(), largeBytes(0, LARGE_SIZE));
  }

  /** Copies the fixture tree under a folder of any provider, for example a bucket. */
  public static void copyTo(FileObject folder) throws IOException {
    for (String path : FILES) {
      FileObject target = folder.resolveFile(path);
      try (InputStream in = open(path);
          OutputStream out = target.getContent().getOutputStream()) {
        in.transferTo(out);
      } finally {
        target.close();
      }
    }
    FileObject large = folder.resolveFile(LARGE_FILE);
    try (OutputStream out = large.getContent().getOutputStream()) {
      out.write(largeBytes(0, LARGE_SIZE));
    } finally {
      large.close();
    }
  }
}
