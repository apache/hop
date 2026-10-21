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
 *
 */

package org.apache.hop.vfs.gs;

/**
 * The {@code gs://} object the current thread is working on, so that a retry reported by {@link
 * LoggingStorageRetryStrategy} can say which file it concerns. The storage client retries on the
 * calling thread, backoff included, which is what makes a thread-local the right reach.
 */
final class GoogleStorageObjectContext {

  private static final ThreadLocal<String> CURRENT = new ThreadLocal<>();

  /** Restores the object the thread was working on before. */
  interface Scope extends AutoCloseable {
    @Override
    void close();
  }

  private GoogleStorageObjectContext() {}

  /**
   * Mark the current thread as working on the given object until the returned scope is closed.
   *
   * @param uri the object, for example {@code gs://bucket/folder/file.txt}; null when unknown
   * @return the scope to close when the work is done
   */
  static Scope enter(String uri) {
    String previous = CURRENT.get();
    CURRENT.set(uri);
    return () -> {
      if (previous == null) {
        CURRENT.remove();
      } else {
        CURRENT.set(previous);
      }
    };
  }

  /**
   * @return the object the current thread is working on, or null when unknown
   */
  static String current() {
    return CURRENT.get();
  }
}
