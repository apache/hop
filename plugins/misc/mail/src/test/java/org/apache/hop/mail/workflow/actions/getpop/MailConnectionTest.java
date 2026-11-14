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
package org.apache.hop.mail.workflow.actions.getpop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.mail.Flags;
import jakarta.mail.Folder;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Store;
import java.io.File;
import java.io.IOException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

/** Unit test for{@link MailConnection} */
class MailConnectionTest {

  private Mconn conn;

  @BeforeEach
  void beforeExec() throws HopException, MessagingException {
    Object subj = new Object();
    ILogChannel log = new LogChannel(subj);
    conn = new Mconn(log);
  }

  @Test
  void constructorStoresConnectionSettings() {
    Assertions.assertEquals("junit", conn.getServer());
    Assertions.assertEquals(0, conn.getPort());
    Assertions.assertEquals("junit", conn.getUsername());
    Assertions.assertFalse(conn.isUseSSL());
    Assertions.assertFalse(conn.isUseXOAUTH2());
    Assertions.assertFalse(conn.isUseProxy());
    Assertions.assertEquals("junit", conn.getProxyUsername());
    Assertions.assertFalse(conn.isConnected());
  }

  /**
   * Test {@link MailConnection#openFolder(String, boolean, boolean)} method. tests that folders are
   * opened recursively
   *
   * @throws HopException
   * @throws MessagingException
   */
  @Test
  void openFolderTest() throws HopException {
    conn.openFolder("a/b", false, false);
    Folder folder = conn.getFolder();
    Assertions.assertEquals("B", folder.getFullName(), "Folder B is opened");
  }

  /**
   * Test {@link MailConnection#setDestinationFolder(String, boolean)} method.
   *
   * @throws HopException
   * @throws MessagingException
   */
  @Test
  void setDestinationFolderTest() throws HopException {
    conn.setDestinationFolder("a/b/c", true);
    assertTrue(conn.cCreated, "Folder C created");
    Assertions.assertEquals(
        Folder.HOLDS_MESSAGES, conn.mode.intValue(), "Folder created with holds messages mode");
  }

  /**
   * Test {@link MailConnection#findValidTarget(String, String) }
   *
   * <p>Note - this test case relies on the ability to create temporary files of zero-byte size in
   * the java.io.tmpdir folder.
   */
  @Test
  void findValidTargetTest() throws IOException, HopException {
    File aFile = null;
    String tmpFileLocation = System.getProperty("java.io.tmpdir");
    String aBaseFile = "pdi17713-.junk";
    tmpFileLocation = tmpFileLocation.replace("\\", "/");
    if (!tmpFileLocation.endsWith("/")) {
      tmpFileLocation = tmpFileLocation + "/";
    }

    // Create temporary files to set up algorithm to have to find an available temp file
    for (int i = 0; i < 3; i++) {
      aFile = new File(tmpFileLocation + "pdi17713-" + i + ".junk");
      if (!aFile.exists()) {
        makeAFile(aFile);
      }
      aFile = new File(tmpFileLocation + "pdi17713-" + i); // no extension version
      if (!aFile.exists()) {
        makeAFile(aFile);
      }
    }

    // **********************************
    // Test with file extensions...
    // **********************************

    // Should now have six files in the tmp folder...
    // with extensions: {tempdir}/pdi17713-0.junk, {tempdir}/pdi17713-1.junk, and
    // {tempdir}/pdi17713-2.junk
    // without extensions: {tempdir}/pdi17713-0, {tempdir}/pdi17713-1, and {tempdir}/pdi17713-2
    String validTargetTestRtn = MailConnection.findValidTarget(tmpFileLocation, aBaseFile);
    // Tests that if the base file doesn't already exist (like IMG00003.png), it will use that one

    assertTrue(validTargetTestRtn.endsWith(aBaseFile), "Original file name should be tried first.");

    // Make sure that the target file already exists so it has to try to find the next available one
    makeAFile(tmpFileLocation + aBaseFile);
    validTargetTestRtn = MailConnection.findValidTarget(tmpFileLocation, aBaseFile);
    // Tests that next available file has a "-3" because 0, 1, and 2 are taken
    assertTrue(
        validTargetTestRtn.endsWith("pdi17713-3.junk"),
        "File extension test failed - expected pdi17713-3.junk as file name");

    // **********************************
    // Now test without file extensions
    // **********************************

    aBaseFile = "pdi17713-";
    validTargetTestRtn = MailConnection.findValidTarget(tmpFileLocation, aBaseFile);
    // Makes sure that it will still use the base file, even with no file extension
    assertTrue(validTargetTestRtn.endsWith(aBaseFile), "Original file name should be tried first.");
    makeAFile(tmpFileLocation + aBaseFile);
    // Make sure that the target file already exists so it has to try to find the next available one
    validTargetTestRtn = MailConnection.findValidTarget(tmpFileLocation, aBaseFile);
    // Tests that next available file has a "-3" because 0, 1, and 2 are taken, even without a file
    // extension
    assertTrue(
        validTargetTestRtn.endsWith("pdi17713-3"),
        "File without extension test failed - expected pdi17713-3.junk as file name");

    assertThrows(
        IllegalArgumentException.class,
        () -> MailConnection.findValidTarget(null, "wibble"),
        "Expected an IllegalArgumentException with a null parameter for folderName to findValidTarget");

    assertThrows(
        IllegalArgumentException.class,
        () -> MailConnection.findValidTarget("wibble", null),
        "Expected an IllegalArgumentException with a null parameter for fileName to findValidTarget");
  }

  /** A message moved on its own counts as moved, not also as deleted. */
  @Test
  void moveMessageCountsAsMovedNotDeleted() throws Exception {
    Message message = Mockito.mock(Message.class);
    when(conn.b.getMessages()).thenReturn(new Message[] {message});
    conn.openFolder("a/b", false, true);
    conn.setDestinationFolder("a/b/c", true);
    conn.retrieveMessages();
    conn.fetchNext();

    conn.moveMessage();

    verify(conn.b).copyMessages(new Message[] {message}, conn.c);
    verify(message).setFlag(Flags.Flag.DELETED, true);
    assertEquals(1, conn.getMovedMessagesCounter());
    assertEquals(0, conn.getDeletedMessagesCounter());
  }

  /** Deleting or moving all messages of several folders adds up the counts of all folders. */
  @Test
  void bulkCountersAddUpOverFolders() throws Exception {
    when(conn.a.getMessages())
        .thenReturn(new Message[] {Mockito.mock(Message.class), Mockito.mock(Message.class)});
    when(conn.b.getMessages()).thenReturn(new Message[] {Mockito.mock(Message.class)});
    conn.setDestinationFolder("a/b/c", true);

    conn.openFolder("a", false, true);
    conn.retrieveMessages();
    conn.deleteMessages(true);
    conn.openFolder("a/b", false, true);
    conn.retrieveMessages();
    conn.deleteMessages(true);
    assertEquals(3, conn.getDeletedMessagesCounter());

    conn.openFolder("a", false, true);
    conn.retrieveMessages();
    conn.moveMessages();
    conn.openFolder("a/b", false, true);
    conn.retrieveMessages();
    conn.moveMessages();
    assertEquals(3, conn.getMovedMessagesCounter());
    assertEquals(3, conn.getDeletedMessagesCounter());
  }

  /** Test {@link MailConnection#folderExists(String)} method. */
  @Test
  void folderExistsTest() {
    boolean actual = conn.folderExists("a/b");
    assertTrue(actual, "Folder B exists");
  }

  private static void makeAFile(String path) throws IOException {
    File aFile = new File(path);
    makeAFile(aFile);
  }

  private static void makeAFile(File aFile) throws IOException {
    aFile.createNewFile(); // makes sure the base file exists so that it will have to use new
    // algorithm
    aFile.deleteOnExit();
  }

  private class Mconn extends MailConnection {

    Store store;
    Folder a;
    Folder b;
    Folder c;
    Folder inbox;

    Integer mode = -1;

    boolean cCreated = false;

    public Mconn(ILogChannel log) throws HopException, MessagingException {
      super(
          log,
          MailConnectionMeta.PROTOCOL_IMAP,
          "junit",
          0,
          "junit",
          "junit",
          false,
          false,
          false,
          "junit");

      store = mock(Store.class);

      inbox = mock(Folder.class);
      a = mock(Folder.class);
      b = mock(Folder.class);
      c = mock(Folder.class);

      when(a.getFullName()).thenReturn("A");
      when(b.getFullName()).thenReturn("B");
      when(c.getFullName()).thenReturn("C");

      when(a.exists()).thenReturn(true);
      when(b.exists()).thenReturn(true);
      when(c.exists()).thenReturn(cCreated);
      when(c.create(Mockito.anyInt()))
          .thenAnswer(
              (Answer<Boolean>)
                  invocation -> {
                    Object arg0 = invocation.getArguments()[0];
                    mode = Integer.class.cast(arg0);
                    cCreated = true;
                    return true;
                  });

      when(inbox.getFolder("a")).thenReturn(a);
      when(a.getFolder("b")).thenReturn(b);
      when(b.getFolder("c")).thenReturn(c);

      when(store.getDefaultFolder()).thenReturn(inbox);
    }

    @Override
    public Store getStore() {
      return this.store;
    }
  }
}
