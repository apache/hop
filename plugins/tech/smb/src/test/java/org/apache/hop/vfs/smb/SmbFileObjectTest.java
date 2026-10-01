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
package org.apache.hop.vfs.smb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.RandomAccessContent;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.util.RandomAccessMode;
import org.junit.jupiter.api.Test;

class SmbFileObjectTest {

  @Test
  void readsWritesListsRenamesAndDeletes() throws Exception {
    MemorySmbShare share = new MemorySmbShare();
    try (Opened manager = opened(share, "")) {
      FileObject folder = manager.resolveFile("finance:///reports");
      folder.createFolder();
      FileObject file = manager.resolveFile("finance:///reports/daily.csv");
      file.createFile();
      assertEquals(null, share.bytes("reports\\daily.csv"));
      write(file, "alpha");
      assertArrayEquals(bytes("alpha"), share.bytes("reports\\daily.csv"));
      try (InputStream in = file.getContent().getInputStream()) {
        assertEquals("alpha", new String(in.readAllBytes(), StandardCharsets.UTF_8));
      }
      assertEquals(0, share.openHandles());
      file.refresh();
      assertTrue(file.getContent().getLastModifiedTime() > 0);

      write(file, "beta");
      try (InputStream in = file.getContent().getInputStream()) {
        assertEquals("beta", new String(in.readAllBytes(), StandardCharsets.UTF_8));
      }
      try (OutputStream out = file.getContent().getOutputStream(true)) {
        out.write(bytes("!"));
      }
      try (InputStream in =
          manager.resolveFile("finance:///reports/daily.csv").getContent().getInputStream()) {
        assertEquals("beta!", new String(in.readAllBytes(), StandardCharsets.UTF_8));
      }

      FileObject[] children = folder.getChildren();
      assertEquals(1, children.length);
      assertTrue(children[0].getName().getBaseName().equals("daily.csv"));

      FileObject renamed = manager.resolveFile("finance:///reports/moved.csv");
      file.moveTo(renamed);
      assertFalse(manager.resolveFile("finance:///reports/daily.csv").exists());
      assertTrue(renamed.exists());
      renamed.delete();
      assertFalse(renamed.exists());
    }
  }

  @Test
  void randomAccessReadsAndWritesAtAnOffset() throws Exception {
    MemorySmbShare share = new MemorySmbShare();
    try (Opened manager = opened(share, "")) {
      FileObject file = manager.resolveFile("finance:///note.txt");
      write(file, "abcd");
      try (RandomAccessContent content =
          file.getContent().getRandomAccessContent(RandomAccessMode.READ)) {
        content.seek(1);
        assertEquals('b', content.readByte());
      }
      assertEquals(0, share.openHandles());
      try (RandomAccessContent content =
          file.getContent().getRandomAccessContent(RandomAccessMode.READWRITE)) {
        content.seek(1);
        content.write('Z');
      }
      assertArrayEquals(bytes("aZcd"), share.bytes("note.txt"));
      assertEquals(0, share.openHandles());
    }
  }

  @Test
  void missingFileIsImaginaryAndAccessDeniedIsNot() throws Exception {
    MemorySmbShare share = new MemorySmbShare();
    share.deny("secret.txt");
    try (Opened manager = opened(share, "")) {
      assertFalse(manager.resolveFile("finance:///missing.txt").exists());
      FileObject denied = manager.resolveFile("finance:///secret.txt");
      assertThrows(FileSystemException.class, denied::exists);
    }
  }

  @Test
  void basePathKeepsFilesInsideTheFolder() throws Exception {
    MemorySmbShare share = new MemorySmbShare();
    try (Opened rooted = opened(share, "restricted")) {
      FileObject file = rooted.resolveFile("finance:///ok.txt");
      write(file, "inside");
      assertArrayEquals(bytes("inside"), share.bytes("restricted\\ok.txt"));
      assertEquals(null, share.bytes("ok.txt"));
      assertThrows(
          FileSystemException.class, () -> rooted.resolveFile("finance:///../../climbed.txt"));
      assertEquals(null, share.bytes("climbed.txt"));
    }
  }

  @Test
  void twoThreadsWriteDifferentFilesOnOneShare() throws Exception {
    MemorySmbShare share = new MemorySmbShare();
    try (Opened manager = opened(share, "")) {
      ExecutorService pool = Executors.newFixedThreadPool(2);
      CountDownLatch start = new CountDownLatch(1);
      try {
        Future<?> first =
            pool.submit(
                () -> {
                  start.await();
                  write(manager.resolveFile("finance:///a.txt"), "one");
                  return null;
                });
        Future<?> second =
            pool.submit(
                () -> {
                  start.await();
                  write(manager.resolveFile("finance:///b.txt"), "two");
                  return null;
                });
        start.countDown();
        first.get();
        second.get();
      } finally {
        pool.shutdownNow();
      }
      assertArrayEquals(bytes("one"), share.bytes("a.txt"));
      assertArrayEquals(bytes("two"), share.bytes("b.txt"));
    }
  }

  private static Opened opened(MemorySmbShare share, String base) throws Exception {
    DefaultFileSystemManager manager = new DefaultFileSystemManager();
    manager.addProvider("finance", new SmbFileProvider(share, base));
    manager.init();
    return new Opened(manager);
  }

  private static final class Opened implements AutoCloseable {
    private final DefaultFileSystemManager manager;

    private Opened(DefaultFileSystemManager manager) {
      this.manager = manager;
    }

    private FileObject resolveFile(String uri) throws FileSystemException {
      return manager.resolveFile(uri);
    }

    @Override
    public void close() {
      manager.close();
    }
  }

  private static void write(FileObject file, String text) throws Exception {
    if (file.exists()) {
      file.delete();
    }
    file.createFile();
    try (OutputStream out = file.getContent().getOutputStream()) {
      out.write(bytes(text));
    }
  }

  private static byte[] bytes(String text) {
    return text.getBytes(StandardCharsets.UTF_8);
  }
}
