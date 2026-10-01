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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** In-memory share for unit tests. Paths use the same {@code \} form as the smbj implementation. */
final class MemorySmbShare implements SmbShare {
  private final Map<String, Node> nodes = new ConcurrentHashMap<>();
  private final AtomicInteger openHandles = new AtomicInteger();

  MemorySmbShare() {
    nodes.put("", Node.directory());
  }

  int openHandles() {
    return openHandles.get();
  }

  void deny(String sharePath) {
    Node node = nodes.get(sharePath);
    if (node == null) {
      node = Node.file();
      nodes.put(sharePath, node);
    }
    node.denied = true;
  }

  byte[] bytes(String sharePath) {
    Node node = nodes.get(sharePath);
    return node == null ? null : Arrays.copyOf(node.data, node.data.length);
  }

  @Override
  public SmbEntry stat(String sharePath) throws IOException {
    Node node = nodes.get(sharePath == null ? "" : sharePath);
    if (node == null) {
      return null;
    }
    if (node.denied) {
      throw new IOException("access denied");
    }
    return node.entry();
  }

  @Override
  public List<String> children(String sharePath) throws IOException {
    String folder = sharePath == null ? "" : sharePath;
    SmbEntry entry = stat(folder);
    if (entry == null) {
      throw new SmbErrors.SmbNotFoundException(folder);
    }
    if (!entry.directory()) {
      throw new IOException("not a folder");
    }
    String prefix = folder.isEmpty() ? "" : folder + "\\";
    List<String> names = new ArrayList<>();
    for (String path : nodes.keySet()) {
      if (path.isEmpty() || path.equals(folder)) {
        continue;
      }
      if (!path.startsWith(prefix)) {
        continue;
      }
      String rest = path.substring(prefix.length());
      if (!rest.isEmpty() && !rest.contains("\\")) {
        names.add(rest);
      }
    }
    return names;
  }

  @Override
  public InputStream openRead(String sharePath) throws IOException {
    Node node = requireFile(sharePath);
    openHandles.incrementAndGet();
    byte[] copy = Arrays.copyOf(node.data, node.data.length);
    return new ByteArrayInputStream(copy) {
      private boolean closed;

      @Override
      public void close() throws IOException {
        if (!closed) {
          closed = true;
          openHandles.decrementAndGet();
        }
        super.close();
      }
    };
  }

  @Override
  public OutputStream openWrite(String sharePath, boolean append) throws IOException {
    Node node = ensureFile(sharePath);
    if (!append) {
      node.data = new byte[0];
    }
    openHandles.incrementAndGet();
    ByteArrayOutputStream buffer =
        new ByteArrayOutputStream() {
          @Override
          public void close() throws IOException {
            byte[] written = toByteArray();
            if (append) {
              byte[] combined = Arrays.copyOf(node.data, node.data.length + written.length);
              System.arraycopy(written, 0, combined, node.data.length, written.length);
              node.data = combined;
            } else {
              node.data = written;
            }
            node.modified = System.currentTimeMillis();
            openHandles.decrementAndGet();
            super.close();
          }
        };
    if (append && node.data.length > 0) {
      buffer.write(node.data);
      node.data = new byte[0];
    }
    return buffer;
  }

  @Override
  public SmbRandom openRandom(String sharePath, boolean write) throws IOException {
    Node node = write ? ensureFile(sharePath) : requireFile(sharePath);
    openHandles.incrementAndGet();
    return new SmbRandom() {
      private boolean closed;

      @Override
      public int read(byte[] buffer, int offset, int length, long fileOffset) {
        if (fileOffset >= node.data.length) {
          return -1;
        }
        int available = (int) Math.min(length, node.data.length - fileOffset);
        System.arraycopy(node.data, (int) fileOffset, buffer, offset, available);
        return available;
      }

      @Override
      public void write(byte[] buffer, int offset, int length, long fileOffset) {
        int end = (int) fileOffset + length;
        if (end > node.data.length) {
          node.data = Arrays.copyOf(node.data, end);
        }
        System.arraycopy(buffer, offset, node.data, (int) fileOffset, length);
        node.modified = System.currentTimeMillis();
      }

      @Override
      public long length() {
        return node.data.length;
      }

      @Override
      public void setLength(long length) {
        node.data = Arrays.copyOf(node.data, (int) length);
      }

      @Override
      public void close() {
        if (!closed) {
          closed = true;
          openHandles.decrementAndGet();
        }
      }
    };
  }

  @Override
  public void createFolder(String sharePath) throws IOException {
    mkdirs(sharePath);
  }

  @Override
  public void delete(String sharePath) throws IOException {
    if (sharePath == null || sharePath.isEmpty()) {
      throw new IOException("Cannot delete the root of an SMB share");
    }
    SmbEntry entry = stat(sharePath);
    if (entry == null) {
      return;
    }
    if (entry.directory()) {
      String prefix = sharePath + "\\";
      for (String path : nodes.keySet()) {
        if (path.startsWith(prefix)) {
          throw new IOException("folder is not empty");
        }
      }
    }
    nodes.remove(sharePath);
  }

  @Override
  public void rename(String sharePath, String newSharePath) throws IOException {
    Node node = nodes.get(sharePath);
    if (node == null || node.denied) {
      throw new SmbErrors.SmbNotFoundException(sharePath);
    }
    nodes.remove(sharePath);
    nodes.put(newSharePath, node);
    String prefix = sharePath + "\\";
    List<String> children = new ArrayList<>();
    for (String path : nodes.keySet()) {
      if (path.startsWith(prefix)) {
        children.add(path);
      }
    }
    for (String path : children) {
      Node child = nodes.remove(path);
      nodes.put(newSharePath + path.substring(sharePath.length()), child);
    }
  }

  @Override
  public void close() {
    // Nothing to release.
  }

  private Node requireFile(String sharePath) throws IOException {
    SmbEntry entry = stat(sharePath);
    if (entry == null) {
      throw new SmbErrors.SmbNotFoundException(sharePath);
    }
    if (entry.directory()) {
      throw new IOException("not a file");
    }
    return nodes.get(sharePath);
  }

  private Node ensureFile(String sharePath) throws IOException {
    mkdirs(parent(sharePath));
    Node existing = nodes.get(sharePath);
    if (existing != null && existing.denied) {
      throw new IOException("access denied");
    }
    if (existing != null && existing.directory) {
      throw new IOException("not a file");
    }
    if (existing == null) {
      existing = Node.file();
      nodes.put(sharePath, existing);
    }
    return existing;
  }

  private void mkdirs(String sharePath) {
    if (sharePath == null || sharePath.isEmpty() || nodes.containsKey(sharePath)) {
      return;
    }
    mkdirs(parent(sharePath));
    nodes.put(sharePath, Node.directory());
  }

  private static String parent(String sharePath) {
    int slash = sharePath.lastIndexOf('\\');
    return slash < 0 ? "" : sharePath.substring(0, slash);
  }

  private static final class Node {
    private boolean directory;
    private byte[] data = new byte[0];
    private long modified = 1_700_000_000_000L;
    private boolean denied;

    private static Node directory() {
      Node node = new Node();
      node.directory = true;
      return node;
    }

    private static Node file() {
      return new Node();
    }

    private SmbEntry entry() {
      return new SmbEntry(directory, directory ? 0 : data.length, modified);
    }
  }
}
