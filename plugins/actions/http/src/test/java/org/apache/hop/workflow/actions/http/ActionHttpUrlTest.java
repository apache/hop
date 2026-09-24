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

package org.apache.hop.workflow.actions.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * The URLs the action accepts. It used to open them with {@link java.net.URLConnection}, which is
 * more lenient than {@link URI}; a URL that worked then must still work now.
 */
class ActionHttpUrlTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private final ActionHttp action = new ActionHttp();

  @BeforeAll
  static void setupBeforeClass() throws Exception {
    // Dropping the user info is logged.
    HopClientEnvironment.init();
  }

  @Test
  void aSpaceInThePathIsEscaped() throws Exception {
    URI uri = action.toUri("http://example.com/a b");

    assertEquals("/a%20b", uri.getRawPath());
  }

  @Test
  void aSpaceInTheQueryIsEscaped() throws Exception {
    URI uri = action.toUri("http://example.com/path?q=a b");

    assertEquals("/path", uri.getRawPath());
    assertEquals("q=a%20b", uri.getRawQuery());
  }

  @Test
  void anEscapedUrlIsNotEscapedTwice() throws Exception {
    URI uri = action.toUri("http://example.com/a%20b?q=x%2Fy");

    assertEquals("http://example.com/a%20b?q=x%2Fy", uri.toString());
  }

  @Test
  void aUserNameAndPasswordInTheUrlAreDropped() throws Exception {
    // URLConnection never sent them, and HttpClient 5 refuses a request URI that carries them.
    URI uri = action.toUri("http://user:secret@example.com:8080/file?x=1#top");

    assertEquals("http://example.com:8080/file?x=1#top", uri.toString());
  }

  @Test
  void aUrlWithoutAHostIsRejected() {
    assertThrows(URISyntaxException.class, () -> action.toUri("localhost:8080/file"));
    assertThrows(URISyntaxException.class, () -> action.toUri("/just/a/path"));
    assertThrows(URISyntaxException.class, () -> action.toUri(""));
  }
}
