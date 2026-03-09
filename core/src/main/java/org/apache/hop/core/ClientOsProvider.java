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

package org.apache.hop.core;

/**
 * Optional provider for the client's operating system when Hop runs in a web context (e.g. Hop Web
 * / RAP). When set, {@link Const#isOSX()} will delegate to this provider so that keyboard shortcuts
 * and labels (e.g. Cmd vs Ctrl) match the user's machine rather than the server's.
 */
public interface ClientOsProvider {

  /**
   * Whether the client is running macOS.
   *
   * @return true if the client OS is macOS, false otherwise
   */
  boolean isClientMac();
}
