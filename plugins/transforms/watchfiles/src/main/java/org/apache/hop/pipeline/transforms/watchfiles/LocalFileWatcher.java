/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.watchfiles;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** No worker threads. One WatchService for the whole tree; closing it wakes the Hop runner. */
public class LocalFileWatcher implements FileWatcher {
  private final Path root;
  private final boolean recursive;
  private final int capacity;
  private final int maximumDirectories;
  private final WatchService service;
  private final Map<WatchKey, Path> keys = new HashMap<>();
  private final Set<Path> registered = new HashSet<>();
  private volatile boolean closed;

  public LocalFileWatcher(Path root, boolean recursive, int capacity, int maximumDirectories)
      throws IOException {
    this.root = root;
    this.recursive = recursive;
    this.capacity = capacity;
    this.maximumDirectories = maximumDirectories;
    service = root.getFileSystem().newWatchService();
    try {
      reconcileDirectories();
    } catch (IOException | RuntimeException e) {
      service.close();
      throw e;
    }
  }

  @Override
  public void reconcileDirectories() throws IOException {
    if (closed) {
      return;
    }
    if (!recursive) {
      register(root);
      return;
    }
    // No FOLLOW_LINKS: recursive symlinks and junctions cannot create loops.
    Files.walkFileTree(
        root,
        new SimpleFileVisitor<>() {
          @Override
          public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attributes)
              throws IOException {
            register(directory);
            return FileVisitResult.CONTINUE;
          }
        });
  }

  private void register(Path directory) throws IOException {
    if (registered.contains(directory)) {
      return;
    }
    if (registered.size() >= maximumDirectories) {
      throw new WatchLimitException("Watch directory limit exceeded. Increase maximum entries.");
    }
    WatchKey key = directory.register(service, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE);
    keys.put(key, directory);
    registered.add(directory);
  }

  @Override
  public WatchBatch poll(long timeoutMillis) throws IOException, InterruptedException {
    Set<Path> paths = new LinkedHashSet<>();
    boolean reconcile = false;
    long overflows = 0;
    try {
      WatchKey key = service.poll(timeoutMillis, TimeUnit.MILLISECONDS);
      // Limit keys per iteration so storms cannot starve stability/checkpoints/stop.
      int remaining = capacity;
      while (key != null && remaining-- > 0 && !closed) {
        Path directory = keys.get(key);
        for (WatchEvent<?> event : key.pollEvents()) {
          if (event.kind() == OVERFLOW) {
            overflows++;
            reconcile = true;
            continue;
          }
          if (directory == null || !(event.context() instanceof Path relative)) {
            reconcile = true;
            continue;
          }
          Path path = directory.resolve(relative).normalize();
          if (Files.isDirectory(path, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            // Creation/rename of a populated directory needs a scan, not just registration.
            if (recursive) {
              reconcile = true;
            }
          } else if (paths.size() < capacity) {
            paths.add(path);
          } else {
            reconcile = true;
          }
          if (event.kind() == ENTRY_DELETE && registered.contains(path)) {
            reconcile = true;
          }
        }
        if (!resetKey(key)) {
          reconcile = true;
        }
        key = service.poll();
      }
      // A key already removed from the service must be processed on the next scan.
      if (key != null) {
        key.pollEvents();
        resetKey(key);
        reconcile = true;
      }
    } catch (ClosedWatchServiceException e) {
      if (!closed) {
        throw e;
      }
    }
    return new WatchBatch(paths, reconcile, overflows);
  }

  private boolean resetKey(WatchKey key) {
    if (key == null) {
      return false;
    }
    if (key.reset()) {
      return true;
    }
    Path removed = keys.remove(key);
    if (removed != null) {
      registered.remove(removed);
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    closed = true;
    service.close();
  }
}
