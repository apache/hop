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
 *
 */

package org.apache.hop.vfs.gs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.HopLoggingEvent;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.gs.config.GoogleCloudConfig;
import org.apache.hop.vfs.gs.config.GoogleCloudConfigSingleton;
import org.apache.hop.vfs.gs.metadatatype.GoogleStorageCredentialsType;
import org.apache.hop.vfs.gs.metadatatype.GoogleStorageMetadataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Credentials that cannot be loaded used to surface as a bare NullPointerException from the Google
 * client on the first {@code gs://} access, and for the default scheme nothing was logged at all.
 * Now the first use fails with an error that says why, logged once.
 */
class GoogleStorageCredentialsTest {

  private static final String MISSING_KEY_FILE = "/nonexistent/hop-gcs-credentials-test-key.json";

  private String originalKeyFile;

  @BeforeAll
  static void initLogging() {
    HopLogStore.init();
  }

  @BeforeEach
  void rememberKeyFile() {
    originalKeyFile = GoogleCloudConfigSingleton.getConfig().getServiceAccountKeyFile();
  }

  @AfterEach
  void restoreKeyFile() {
    GoogleCloudConfigSingleton.getConfig().setServiceAccountKeyFile(originalKeyFile);
  }

  @Test
  void aKeyFileThatCannotBeReadFailsClearlyOnFirstUse() throws Exception {
    GoogleCloudConfig config = GoogleCloudConfigSingleton.getConfig();
    config.setServiceAccountKeyFile(MISSING_KEY_FILE);

    GoogleStorageFileSystem fileSystem = fileSystem(new GoogleStorageFileProvider(), "gs");

    IOException error = assertThrows(IOException.class, fileSystem::setupStorage);
    String message = error.getMessage();
    assertTrue(
        message.startsWith("Google Cloud Storage: no credentials to access gs://: "), message);
    assertTrue(
        message.contains("the service account key file '" + MISSING_KEY_FILE + "'"), message);
    assertTrue(message.contains("FileNotFoundException"), message);

    // Every use fails the same way, but the log says it only once.
    assertThrows(IOException.class, fileSystem::setupStorage);
    assertThrows(IOException.class, fileSystem::getStorageControlClient);
    assertEquals(1, loggedLinesContaining(MISSING_KEY_FILE));
  }

  @Test
  void aNamedConnectionWithBrokenCredentialsFailsClearly() {
    GoogleStorageMetadataType connection = new GoogleStorageMetadataType();
    connection.setName("broken-gcs");
    connection.setStorageCredentialsType(GoogleStorageCredentialsType.KEY_STRING);
    connection.setStorageAccountKey("this is not a service account key");

    GoogleStorageFileSystem fileSystem =
        fileSystem(new GoogleStorageFileProvider(new Variables(), connection), "broken-gcs");

    IOException error = assertThrows(IOException.class, fileSystem::setupStorage);
    String message = error.getMessage();
    assertTrue(
        message.startsWith("Google Cloud Storage: no credentials to access broken-gcs://: "),
        message);
    assertTrue(
        message.contains(
            "the credentials of Google Storage connection 'broken-gcs' could not be loaded"),
        message);
  }

  private static GoogleStorageFileSystem fileSystem(
      GoogleStorageFileProvider provider, String scheme) {
    try {
      return (GoogleStorageFileSystem)
          provider.doCreateFileSystem(
              new GoogleStorageFileName(scheme, "/", FileType.FOLDER), new FileSystemOptions());
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static long loggedLinesContaining(String text) {
    return HopLogStore.getAppender()
        .getLogBufferFromTo((java.util.List<String>) null, true, 0, Integer.MAX_VALUE)
        .stream()
        .map(HopLoggingEvent::getMessage)
        .map(String::valueOf)
        .filter(line -> line.contains(text))
        .count();
  }
}
