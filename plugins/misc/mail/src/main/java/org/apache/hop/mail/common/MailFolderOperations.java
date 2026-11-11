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

import jakarta.mail.Flags;
import jakarta.mail.Folder;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import lombok.experimental.UtilityClass;

/**
 * Moving, deleting and listing messages and folders, shared by {@code MailServerConnection} and the
 * legacy {@code MailConnection}. Each method returns the number of messages to add to the caller's
 * counter, and only once the whole operation succeeded. A moved message counts as moved, not also
 * as deleted.
 */
@UtilityClass
public class MailFolderOperations {

  /**
   * Flag one message as deleted.
   *
   * @return the number of deleted messages (1)
   */
  public static int deleteMessage(Message message) throws MessagingException {
    message.setFlag(Flags.Flag.DELETED, true);
    return 1;
  }

  /**
   * Flag messages of a folder as deleted.
   *
   * @return the number of deleted messages
   */
  public static int deleteMessages(Folder folder, Message[] messages) throws MessagingException {
    folder.setFlags(messages, new Flags(Flags.Flag.DELETED), true);
    return messages.length;
  }

  /**
   * Copy one message to the destination folder and flag the original as deleted.
   *
   * @return the number of moved messages (1)
   */
  public static int moveMessage(Folder source, Message message, Folder destination)
      throws MessagingException {
    source.copyMessages(new Message[] {message}, destination);
    deleteMessage(message);
    return 1;
  }

  /**
   * Copy messages to the destination folder and flag the originals as deleted.
   *
   * @return the number of moved messages
   */
  public static int moveMessages(Folder source, Message[] messages, Folder destination)
      throws MessagingException {
    source.copyMessages(messages, destination);
    deleteMessages(source, messages);
    return messages.length;
  }

  /**
   * Full names of all folders below a folder, depth first and sorted by name on each level, so the
   * order is the same in every run. Leaf folders that cannot hold subfolders are included too.
   *
   * @param folder the parent folder
   * @return the full names of the subfolders
   */
  public static List<String> listSubfolders(Folder folder) throws MessagingException {
    List<String> names = new ArrayList<>();
    addSubfolders(folder, names);
    return names;
  }

  private static void addSubfolders(Folder folder, List<String> names) throws MessagingException {
    if ((folder.getType() & Folder.HOLDS_FOLDERS) == 0) {
      return;
    }
    Folder[] children = folder.list();
    Arrays.sort(children, Comparator.comparing(Folder::getFullName));
    for (Folder child : children) {
      if ((child.getType() & (Folder.HOLDS_FOLDERS | Folder.HOLDS_MESSAGES)) != 0) {
        names.add(child.getFullName());
        addSubfolders(child, names);
      }
    }
  }
}
