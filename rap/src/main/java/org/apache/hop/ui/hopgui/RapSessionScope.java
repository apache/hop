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

package org.apache.hop.ui.hopgui;

import org.apache.hop.core.scope.IHopScope;
import org.eclipse.rap.rwt.SingletonUtil;

/**
 * Holds one piece of Hop's ambient state per RAP session, so that one user's project does not
 * become another's. This is what {@link org.apache.hop.metadata.util.HopMetadataInstance} and
 * {@link org.apache.hop.core.vfs.HopVfsNamespaces} keep their "current" value in when Hop runs as
 * Hop Web.
 *
 * <p>Not every thread that asks has a RAP session: an execution runs on threads Hop started itself,
 * and those have no request and no {@code UISession}. So the value is kept twice - in the session
 * for whoever is serving a request, and in a thread local inherited by threads an execution starts.
 * A pipeline launched from the GUI therefore keeps resolving in the session that launched it, while
 * a second session serving a request alongside it gets its own.
 *
 * <p>One instance per kind of state, each with its own holder class: {@link SingletonUtil} keys the
 * per session instance on that class.
 *
 * @param <T> the type of the value held
 */
public class RapSessionScope<T> implements IHopScope<T> {

  /** Kept per RAP session by {@link SingletonUtil}. One subclass per kind of state. */
  public static class Holder {
    Object value;
  }

  /** The session slot for the metadata provider in use. */
  public static class MetadataProviderHolder extends Holder {}

  /** The session slot for the VFS namespace files are resolved in. */
  public static class VfsNamespaceHolder extends Holder {}

  private final Class<? extends Holder> holderType;

  /** For the threads of an execution, which have no session to ask. */
  private final InheritableThreadLocal<T> outsideASession = new InheritableThreadLocal<>();

  public RapSessionScope(Class<? extends Holder> holderType) {
    this.holderType = holderType;
  }

  @Override
  public T get() {
    return valueOf(holder());
  }

  /**
   * The answer for a given session slot. Kept apart from {@link #get()} so it can be exercised
   * without a RAP session, which is the only way to reach the case where a session exists but has
   * nothing bound.
   *
   * @param holder the session's slot, or null when this thread is serving no session
   * @return the value in scope
   */
  @SuppressWarnings("unchecked")
  T valueOf(Holder holder) {
    if (holder != null) {
      // Whether this session has anything bound or not, the answer is its own. Falling back to the
      // thread here would hand it whatever the previous session on this pooled request thread
      // left behind.
      return (T) holder.value;
    }
    return outsideASession.get();
  }

  @Override
  public void set(T value) {
    Holder holder = holder();
    if (holder != null) {
      holder.value = value;
    }
    // Also outside the session: an execution started from here runs on its own threads, and they
    // have to keep resolving the way the session that launched them does.
    outsideASession.set(value);
  }

  @Override
  public void remove() {
    Holder holder = holder();
    if (holder != null) {
      holder.value = null;
    }
    outsideASession.remove();
  }

  /** The session's holder, or null when this thread is not serving a session. */
  private Holder holder() {
    try {
      return SingletonUtil.getSessionInstance(holderType);
    } catch (Exception e) {
      // No UISession on this thread: an execution thread, or a shutdown hook.
      return null;
    }
  }
}
