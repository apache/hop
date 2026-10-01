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
 * Decides whether a dialog subject should open in read-only mode for the current security context.
 */
public final class HopDialogEditGuard {

  private HopDialogEditGuard() {}

  /**
   * @param subject object attached to the shell (typically {@link IDialogEditable}), or null
   * @return true if the dialog must not accept edits
   */
  public static boolean isReadOnly(Object subject) {
    if (subject == null) {
      return false;
    }
    if (subject instanceof IDialogEditable editable) {
      Permission permission = editable.requiredEditPermission();
      if (permission == null) {
        return false;
      }
      return !HopSecurity.allows(permission);
    }
    return false;
  }

  /**
   * @param subject dialog subject
   * @return required permission, or null if not gated
   */
  public static Permission requiredPermission(Object subject) {
    if (subject instanceof IDialogEditable editable) {
      return editable.requiredEditPermission();
    }
    return null;
  }
}
