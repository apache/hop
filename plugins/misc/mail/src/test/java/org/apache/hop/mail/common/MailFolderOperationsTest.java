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

package org.apache.hop.mail.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.mail.Flags;
import jakarta.mail.Folder;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import java.util.List;
import org.junit.jupiter.api.Test;

class MailFolderOperationsTest {

  @Test
  void moveMessageCopiesFlagsAndCountsOne() throws Exception {
    Folder source = mock(Folder.class);
    Folder destination = mock(Folder.class);
    Message message = mock(Message.class);

    assertEquals(1, MailFolderOperations.moveMessage(source, message, destination));

    verify(source).copyMessages(new Message[] {message}, destination);
    verify(message).setFlag(Flags.Flag.DELETED, true);
  }

  /** A copied message whose original could not be flagged is not reported as moved. */
  @Test
  void moveMessageThrowsWhenFlaggingFails() throws Exception {
    Folder source = mock(Folder.class);
    Message message = mock(Message.class);
    doThrow(new MessagingException("read-only")).when(message).setFlag(Flags.Flag.DELETED, true);

    assertThrows(
        MessagingException.class,
        () -> MailFolderOperations.moveMessage(source, message, mock(Folder.class)));
  }

  @Test
  void moveMessagesDoesNotCopyWhenNothingFlagged() throws Exception {
    Folder source = mock(Folder.class);
    Folder destination = mock(Folder.class);
    Message[] messages = {mock(Message.class), mock(Message.class)};
    doThrow(new MessagingException("down")).when(source).copyMessages(messages, destination);

    assertThrows(
        MessagingException.class,
        () -> MailFolderOperations.moveMessages(source, messages, destination));
    verify(source, never()).setFlags(eq(messages), any(Flags.class), eq(true));
  }

  @Test
  void moveMessagesCountsAll() throws Exception {
    Folder source = mock(Folder.class);
    Folder destination = mock(Folder.class);
    Message[] messages = {mock(Message.class), mock(Message.class)};

    assertEquals(2, MailFolderOperations.moveMessages(source, messages, destination));

    verify(source).copyMessages(messages, destination);
    verify(source).setFlags(messages, new Flags(Flags.Flag.DELETED), true);
  }

  /**
   * Subfolders come back depth first and sorted on each level, whatever order the server lists them
   * in, and leaf folders that cannot hold subfolders (\Noinferiors) are included.
   */
  @Test
  void listSubfoldersIsSortedAndIncludesLeafFolders() throws Exception {
    Folder z = folder("INBOX.z", Folder.HOLDS_MESSAGES);
    Folder bx = folder("INBOX.b.x", Folder.HOLDS_MESSAGES | Folder.HOLDS_FOLDERS);
    Folder b = folder("INBOX.b", Folder.HOLDS_MESSAGES | Folder.HOLDS_FOLDERS, bx);
    Folder a = folder("INBOX.a", Folder.HOLDS_MESSAGES);
    Folder inbox = folder("INBOX", Folder.HOLDS_MESSAGES | Folder.HOLDS_FOLDERS, z, b, a);

    List<String> names = MailFolderOperations.listSubfolders(inbox);

    assertEquals(List.of("INBOX.a", "INBOX.b", "INBOX.b.x", "INBOX.z"), names);
    verify(a, never()).list();
    verify(z, never()).list();
  }

  private static Folder folder(String fullName, int type, Folder... children)
      throws MessagingException {
    Folder folder = mock(Folder.class);
    when(folder.getFullName()).thenReturn(fullName);
    when(folder.getType()).thenReturn(type);
    when(folder.list()).thenReturn(children);
    return folder;
  }
}
