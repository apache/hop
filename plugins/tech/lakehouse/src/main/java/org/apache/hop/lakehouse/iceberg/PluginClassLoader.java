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

package org.apache.hop.lakehouse.iceberg;

/**
 * Makes this plugin's class loader the thread's context class loader for a while.
 *
 * <p>Iceberg finds parts of itself through the context class loader, for example the file format
 * models it registers once, the first time a reader or writer is built. In Hop the context class
 * loader of a transform thread is Hop's own, which can't see the jars of this plugin, so every call
 * into Iceberg is wrapped:
 *
 * <pre>{@code
 * try (PluginClassLoader ignored = PluginClassLoader.activate()) {
 *   ...
 * }
 * }</pre>
 */
public final class PluginClassLoader implements AutoCloseable {

  private final Thread thread;
  private final ClassLoader previous;

  private PluginClassLoader() {
    thread = Thread.currentThread();
    previous = thread.getContextClassLoader();
    thread.setContextClassLoader(PluginClassLoader.class.getClassLoader());
  }

  public static PluginClassLoader activate() {
    return new PluginClassLoader();
  }

  @Override
  public void close() {
    thread.setContextClassLoader(previous);
  }
}
