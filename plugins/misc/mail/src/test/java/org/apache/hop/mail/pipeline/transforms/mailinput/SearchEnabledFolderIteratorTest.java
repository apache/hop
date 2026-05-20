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
package org.apache.hop.mail.pipeline.transforms.mailinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import jakarta.mail.Message;
import jakarta.mail.search.SearchTerm;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import org.junit.jupiter.api.Test;

class SearchEnabledFolderIteratorTest {

  private static SearchTerm matching(Message... matches) {
    return new SearchTerm() {
      @Override
      public boolean match(Message msg) {
        for (Message m : matches) {
          if (m == msg) {
            return true;
          }
        }
        return false;
      }
    };
  }

  @Test
  void emptyIteratorHasNoElements() {
    SearchEnabledFolderIterator it =
        new SearchEnabledFolderIterator(Collections.<Message>emptyIterator(), matching());

    assertFalse(it.hasNext());
  }

  @Test
  void filtersOutNonMatchingMessages() {
    Message keep1 = mock(Message.class);
    Message skip1 = mock(Message.class);
    Message keep2 = mock(Message.class);
    Message skip2 = mock(Message.class);
    Iterator<Message> source = Arrays.asList(skip1, keep1, skip2, keep2).iterator();

    SearchEnabledFolderIterator it =
        new SearchEnabledFolderIterator(source, matching(keep1, keep2));

    assertTrue(it.hasNext());
    assertSame(keep1, it.next());
    assertTrue(it.hasNext());
    assertSame(keep2, it.next());
    assertFalse(it.hasNext());
  }

  @Test
  void allNonMatchingDrainsSource() {
    Message m1 = mock(Message.class);
    Message m2 = mock(Message.class);
    Iterator<Message> source = Arrays.asList(m1, m2).iterator();

    SearchEnabledFolderIterator it = new SearchEnabledFolderIterator(source, matching());

    assertFalse(it.hasNext());
    // Verify the source was fully drained looking for matches.
    assertFalse(source.hasNext());
  }

  @Test
  void allMatchingReturnsAll() {
    Message m1 = mock(Message.class);
    Message m2 = mock(Message.class);
    Message m3 = mock(Message.class);
    Iterator<Message> source = Arrays.asList(m1, m2, m3).iterator();

    SearchEnabledFolderIterator it = new SearchEnabledFolderIterator(source, matching(m1, m2, m3));

    int count = 0;
    while (it.hasNext()) {
      it.next();
      count++;
    }
    assertEquals(3, count);
  }

  @Test
  void removeIsUnsupported() {
    SearchEnabledFolderIterator it =
        new SearchEnabledFolderIterator(Collections.<Message>emptyIterator(), matching());

    assertThrows(UnsupportedOperationException.class, it::remove);
  }
}
