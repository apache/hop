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

package org.apache.hop.core.security;

/**
 * Marker for Hop objects that are edited in dialogs (transform metas, actions, metadata, …).
 *
 * <p>When such an object is attached to a dialog shell (UI key {@code hop.dialog.subject}), {@code
 * BaseDialog.defaultShellHandling} makes the shell read-only if the current user lacks {@link
 * #requiredEditPermission()}.
 *
 * <p>Implemented on shared bases ({@code BaseTransformMeta}, {@code ActionBase}, {@code
 * HopMetadataBase}) so plugins inherit the behaviour without per-class code.
 */
public interface IDialogEditable {

  /**
   * Permission required to change this object in a dialog.
   *
   * @return permission, or {@code null} if the dialog should always stay editable
   */
  Permission requiredEditPermission();
}
