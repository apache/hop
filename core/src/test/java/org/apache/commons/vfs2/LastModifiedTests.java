/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.commons.vfs2;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import org.junit.jupiter.api.Test;

/** Test cases for getting and setting file last modified time. */
public class LastModifiedTests extends AbstractProviderTestCase {

  protected static final Duration ONE_DAY = Duration.ofDays(1);

  protected void assertDeltaMillis(
      final String message,
      final long expectedMillis,
      final long actualMillis,
      final long deltaMillis) {
    if (expectedMillis == actualMillis) {
      return;
    }
    // getLastModTimeAccuracy() is not accurate
    final long actualDelta = Math.abs(expectedMillis - actualMillis);
    if (actualDelta > Math.max(deltaMillis, 1000)) {
      fail(
          String.format(
              "%s expected=%,d (%s), actual=%,d (%s), expected delta=%,d millis, actual delta=%,d millis",
              message,
              Long.valueOf(expectedMillis),
              new Date(expectedMillis).toString(),
              Long.valueOf(actualMillis),
              new Date(actualMillis).toString(),
              Long.valueOf(deltaMillis),
              Long.valueOf(actualDelta)));
    }
  }

  protected void assertEqualMillis(
      final String message, final long expectedMillis, final long actualMillis) {
    if (expectedMillis != actualMillis) {
      final long delta = Math.abs(expectedMillis - actualMillis);
      fail(
          String.format(
              "%s expected=%,d (%s), actual=%,d (%s), delta=%,d millis",
              message,
              Long.valueOf(expectedMillis),
              new Date(expectedMillis).toString(),
              Long.valueOf(actualMillis),
              new Date(actualMillis).toString(),
              delta));
    }
  }

  /** Returns the capabilities required by the tests of this test case. */
  @Override
  protected Capability[] getRequiredCapabilities() {
    return new Capability[] {Capability.GET_LAST_MODIFIED};
  }

  /**
   * Tests FileSystem#getLastModTimeAccuracy for sane values.
   *
   * @throws FileSystemException if error occurred
   */
  @Test
  public void testGetAccuracy() throws FileSystemException {
    final FileObject file = getReadFolder().resolveFile("file1.txt");
    final long lastModTimeAccuracyMillis = (long) file.getFileSystem().getLastModTimeAccuracy();
    // System.out.println("Accuracy on " + file.getFileSystem().getRootURI() + " is " +
    // lastModTimeAccuracy + " as
    // told by " + file.getFileSystem().getClass().getCanonicalName());
    assertTrue(lastModTimeAccuracyMillis >= 0, "Accuracy must be positive");
    // just any sane limit
    assertTrue(
        lastModTimeAccuracyMillis < Duration.ofMinutes(2).toMillis(), "Accuracy must be < 2m");
  }

  /**
   * Tests getting the last modified time of a file.
   *
   * @throws FileSystemException if error occurred
   */
  @Test
  public void testGetLastModifiedFile() throws FileSystemException {
    final FileObject file = getReadFolder().resolveFile("file1.txt");
    assertNotEquals(0L, file.getContent().getLastModifiedTime());
  }

  /**
   * Tests getting the last modified time of a folder.
   *
   * @throws FileSystemException if error occurred
   */
  @Test
  public void testGetLastModifiedFolder() throws FileSystemException {
    final FileObject file = getReadFolder().resolveFile("dir1");
    assertNotEquals(0L, file.getContent().getLastModifiedTime());
  }

  /**
   * Tests setting the last modified time of file.
   *
   * @throws FileSystemException if error occurred
   */
  @Test
  public void testSetLastModifiedFile() throws FileSystemException {
    final long yesterdayMillis = Instant.now().minus(ONE_DAY).toEpochMilli();

    if (getReadFolder().getFileSystem().hasCapability(Capability.SET_LAST_MODIFIED_FILE)) {
      // Try a file
      final FileObject file = getReadFolder().resolveFile("file1.txt");
      file.getContent().setLastModifiedTime(yesterdayMillis);
      final long lastModTimeAccuracyMillis = (long) file.getFileSystem().getLastModTimeAccuracy();
      // folder.refresh(); TODO: does not work with SSH VFS-563
      final long lastModifiedTime = file.getContent().getLastModifiedTime();
      assertDeltaMillis(
          "set/getLastModified on File",
          yesterdayMillis,
          lastModifiedTime,
          lastModTimeAccuracyMillis);
    }
  }

  /**
   * Tests setting the last modified time of a folder.
   *
   * @throws FileSystemException if error occurred
   */
  @Test
  public void testSetLastModifiedFolder() throws FileSystemException {
    final long yesterdayMillis = Instant.now().minus(ONE_DAY).toEpochMilli();

    if (getReadFolder().getFileSystem().hasCapability(Capability.SET_LAST_MODIFIED_FOLDER)) {
      // Try a folder
      final FileObject folder = getReadFolder().resolveFile("dir1");
      folder.getContent().setLastModifiedTime(yesterdayMillis);
      final long lastModTimeAccuracyMillis = (long) folder.getFileSystem().getLastModTimeAccuracy();
      // folder.refresh(); TODO: does not work with SSH VFS-563
      final long lastModifiedTime = folder.getContent().getLastModifiedTime();
      assertDeltaMillis(
          "set/getLastModified on Folder",
          yesterdayMillis,
          lastModifiedTime,
          lastModTimeAccuracyMillis);
    }
  }
}
