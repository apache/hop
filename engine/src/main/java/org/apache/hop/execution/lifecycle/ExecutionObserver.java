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

/**
 * Receives execution events without coupling the engine to a monitoring implementation. Callbacks
 * must be short and must not wait for another thread to enter the lifecycle. They may reenter it on
 * the same thread. Failures are contained by the lifecycle. An observer whose start fails receives
 * no further callbacks for that execution and must clean up any resources it created before
 * throwing. Likewise, activate must clean up resources it has not yet returned as a scope if it
 * throws.
 */
@FunctionalInterface
public interface ExecutionObserver {
  void started(ExecutionHandle handle);

  /** Returns a same-thread scope, or null when no observer context needs activation. */
  default ExecutionScope activate(ExecutionHandle handle) {
    return null;
  }

  default void finished(ExecutionHandle handle, ExecutionState state, Throwable failure) {}
}
