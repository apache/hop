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
 */
package org.apache.hop.execution.lifecycle;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Immutable execution identity and parent link, with a mutable, thread-safe attachment store.
 * Finishing detaches a handle from the active registry, not from its children or attachments.
 * Retained children can therefore use the exact parent context after the parent finishes. Observers
 * should use private identity keys and retain only context they need, not live scopes or resources
 * requiring finish-time cleanup. Attachments live as long as the handle (or until explicitly
 * removed).
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public final class ExecutionHandle {
  @Getter private final UUID id = UUID.randomUUID();
  @Getter private final ExecutionHandle parent;
  @Getter private final Object subject;
  @Getter private final ExecutionKind kind;
  @Getter private final String name;
  @Getter private final int copy;

  private final Map<Object, Object> attachments = new IdentityHashMap<>();
  private final ReentrantLock attachmentLock = new ReentrantLock();

  /** Looks up an observer-owned key by identity, without invoking its equals or hashCode. */
  public Object getAttachment(Object key) {
    attachmentLock.lock();
    try {
      return attachments.get(key);
    } finally {
      attachmentLock.unlock();
    }
  }

  /** Stores context under an observer-owned identity key; null removes the attachment. */
  public void setAttachment(Object key, Object value) {
    attachmentLock.lock();
    try {
      if (value == null) {
        attachments.remove(key);
      } else {
        attachments.put(key, value);
      }
    } finally {
      attachmentLock.unlock();
    }
  }
}
