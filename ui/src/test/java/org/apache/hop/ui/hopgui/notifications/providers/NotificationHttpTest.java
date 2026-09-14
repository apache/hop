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
package org.apache.hop.ui.hopgui.notifications.providers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

/** What the notification providers are willing to request, and how much of it they will read. */
public class NotificationHttpTest {

  @Test
  public void testHttpAndHttpsAreRequested() throws Exception {
    assertEquals(
        URI.create("https://hop.apache.org/feed.xml"),
        NotificationHttp.requestable("https://hop.apache.org/feed.xml"));
    assertEquals(
        URI.create("http://example.org/rss"),
        NotificationHttp.requestable("http://example.org/rss"));
  }

  @Test
  public void testSurroundingWhitespaceIsIgnored() throws Exception {
    assertEquals(
        URI.create("https://hop.apache.org/feed.xml"),
        NotificationHttp.requestable("  https://hop.apache.org/feed.xml  "));
  }

  @Test
  public void testOtherSchemesAreRefused() {
    // A stored source is whatever the user typed, and on Hop Web it is the server that reads it.
    for (String url :
        new String[] {
          "file:///etc/passwd", "ftp://example.org/feed.xml", "jar:file:///tmp/x.jar!/feed.xml"
        }) {
      assertThrows(HopException.class, () -> NotificationHttp.requestable(url), url);
    }
  }

  @Test
  public void testUrlWithoutAHostIsRefused() {
    assertThrows(HopException.class, () -> NotificationHttp.requestable("https:///feed.xml"));
    assertThrows(HopException.class, () -> NotificationHttp.requestable("not a url at all"));
    assertThrows(HopException.class, () -> NotificationHttp.requestable(null));
  }

  @Test
  public void testAResponseWithinTheLimitIsReadWhole() throws Exception {
    byte[] body = new byte[1024];
    try (InputStream stream =
        NotificationHttp.bounded(new ByteArrayInputStream(body), "https://example.org/feed.xml")) {
      assertEquals(body.length, stream.readAllBytes().length);
    }
  }

  @Test
  public void testAResponseOverTheLimitFails() {
    // Neither provider knows how much it is about to read: the feed goes to a DOM parser and the
    // GitHub answer to Jackson, so an endless response would otherwise take the process with it.
    InputStream endless =
        new InputStream() {
          @Override
          public int read() {
            return 'a';
          }

          @Override
          public int read(byte[] buffer, int offset, int length) {
            java.util.Arrays.fill(buffer, offset, offset + length, (byte) 'a');
            return length;
          }
        };
    IOException e =
        assertThrows(
            IOException.class,
            () -> NotificationHttp.bounded(endless, "https://example.org/feed.xml").readAllBytes());
    assertTrue(e.getMessage().contains("https://example.org/feed.xml"), e.getMessage());
  }

  @Test
  public void testAVariableExpressionIsResolvedAtTheMomentOfUse() {
    // Storing the reference rather than the token keeps it out of hop-config.json.
    assertEquals("plain-token", NotificationHttp.resolve("plain-token"));
    assertEquals(null, NotificationHttp.resolve(null));
  }

  @Test
  public void testATokenOnlySourceStillBuildsAClient() {
    // HttpClient 5 will not build a credential without a user name, and a source may store only a
    // token. Before the user name was coerced this threw on the first poll.
    assertNotNull(
        NotificationHttp.newClient(URI.create("https://api.github.com/x"), null, "a-token"));
    assertNotNull(NotificationHttp.newClient(URI.create("https://example.org/rss"), "", "a-token"));
    assertNotNull(NotificationHttp.newClient());
  }

  @Test
  public void testStringsThatAreNotUrlsAreRefusedRatherThanRequested() {
    assertThrows(HopException.class, () -> NotificationHttp.requestable(""));
    assertThrows(HopException.class, () -> NotificationHttp.requestable("   "));
  }

  @Test
  public void testBoundedStreamCountsSingleByteReads() throws Exception {
    byte[] body = "hello".getBytes(StandardCharsets.UTF_8);
    try (InputStream stream =
        NotificationHttp.bounded(new ByteArrayInputStream(body), "https://example.org/x")) {
      int count = 0;
      while (stream.read() != -1) {
        count++;
      }
      assertEquals(body.length, count);
    }
  }
}
