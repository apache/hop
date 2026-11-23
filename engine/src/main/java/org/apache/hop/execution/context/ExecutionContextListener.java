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
package org.apache.hop.execution.context;

/**
 * Activates listener-owned context for an existing Hop log channel on the calling thread. This is
 * not an execution start or completion notification. Callbacks may be concurrent and reentrant.
 */
@FunctionalInterface
public interface ExecutionContextListener {
  /**
   * Returns restoration for this activation, or {@code null} if none is needed. The scope is closed
   * on this thread, even if the listener is subsequently unregistered. Implementations must undo
   * any partial activation themselves if this callback throws before returning a scope.
   *
   * @param logChannelId the existing, non-null log channel ID; no registry lookup is performed
   */
  ExecutionScope activate(String logChannelId);
}
