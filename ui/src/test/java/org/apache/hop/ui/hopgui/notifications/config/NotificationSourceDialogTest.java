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
package org.apache.hop.ui.hopgui.notifications.config;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for reading a repository out of the URL field. This is what keeps the URL and the
 * owner and repository fields in step, so editing whichever one you reach for first is the one that
 * counts.
 */
public class NotificationSourceDialogTest {

  @Test
  public void testFullUrl() {
    assertArrayEquals(
        new String[] {"apache", "hop"},
        NotificationSourceDialog.parseOwnerAndRepo("https://github.com/apache/hop"));
  }

  @Test
  public void testUrlWithTrailingPath() {
    assertArrayEquals(
        new String[] {"apache", "hop"},
        NotificationSourceDialog.parseOwnerAndRepo("https://github.com/apache/hop/releases"));
  }

  @Test
  public void testUrlWithQueryAndFragment() {
    assertArrayEquals(
        new String[] {"apache", "hop"},
        NotificationSourceDialog.parseOwnerAndRepo("https://github.com/apache/hop?tab=readme#top"));
  }

  @Test
  public void testShorthand() {
    assertArrayEquals(
        new String[] {"apache", "hop"}, NotificationSourceDialog.parseOwnerAndRepo("apache/hop"));
  }

  @Test
  public void testTypingTheRepositoryIsFollowedCharacterByCharacter() {
    // The sync runs on every keystroke, so a half-typed repository still has to read cleanly.
    assertArrayEquals(
        new String[] {"apache", "h"},
        NotificationSourceDialog.parseOwnerAndRepo("https://github.com/apache/h"));
    assertArrayEquals(
        new String[] {"apache", "hopp"},
        NotificationSourceDialog.parseOwnerAndRepo("https://github.com/apache/hopp"));
  }

  @Test
  public void testIncompleteInputLeavesTheFieldsAlone() {
    // Returning null means "do not touch the owner and repository", not "clear them".
    assertNull(NotificationSourceDialog.parseOwnerAndRepo(null));
    assertNull(NotificationSourceDialog.parseOwnerAndRepo(""));
    assertNull(NotificationSourceDialog.parseOwnerAndRepo("   "));
    assertNull(NotificationSourceDialog.parseOwnerAndRepo("https://github.com/"));
    assertNull(NotificationSourceDialog.parseOwnerAndRepo("https://github.com/apache"));
    assertNull(NotificationSourceDialog.parseOwnerAndRepo("apache"));
    assertNull(NotificationSourceDialog.parseOwnerAndRepo("https://github.com/apache/"));
  }
}
