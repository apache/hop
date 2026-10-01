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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class HopDialogEditGuardTest {

  @AfterEach
  void resetProvider() {
    HopSecurity.reset();
  }

  @Test
  void nullSubjectIsNotReadOnly() {
    assertFalse(HopDialogEditGuard.isReadOnly(null));
    assertNull(HopDialogEditGuard.requiredPermission(null));
  }

  @Test
  void nonEditableSubjectIsNotReadOnly() {
    assertFalse(HopDialogEditGuard.isReadOnly("plain string"));
    assertNull(HopDialogEditGuard.requiredPermission("plain string"));
  }

  @Test
  void unrestrictedContextKeepsDialogEditable() {
    HopSecurity.reset();
    IDialogEditable subject = () -> Permission.FILE_EDIT;
    assertFalse(HopDialogEditGuard.isReadOnly(subject));
    assertEquals(Permission.FILE_EDIT, HopDialogEditGuard.requiredPermission(subject));
  }

  @Test
  void readOnlyRoleBlocksFileEdit() {
    HopSecurity.setProvider(() -> HopSecurityContext.forUser("viewer", Set.of(HopRole.READ_ONLY)));
    IDialogEditable subject = () -> Permission.FILE_EDIT;
    assertTrue(HopDialogEditGuard.isReadOnly(subject));
  }

  @Test
  void readOnlyRoleBlocksMetadataWrite() {
    HopSecurity.setProvider(() -> HopSecurityContext.forUser("viewer", Set.of(HopRole.READ_ONLY)));
    IDialogEditable subject = () -> Permission.METADATA_WRITE;
    assertTrue(HopDialogEditGuard.isReadOnly(subject));
  }

  @Test
  void operatorCanViewButNotEditTransforms() {
    HopSecurity.setProvider(() -> HopSecurityContext.forUser("ops", Set.of(HopRole.OPERATOR)));
    IDialogEditable transformMeta = () -> Permission.FILE_EDIT;
    IDialogEditable metadata = () -> Permission.METADATA_WRITE;
    assertTrue(HopDialogEditGuard.isReadOnly(transformMeta));
    assertTrue(HopDialogEditGuard.isReadOnly(metadata));
  }

  @Test
  void userCanEdit() {
    HopSecurity.setProvider(() -> HopSecurityContext.forUser("dev", Set.of(HopRole.USER)));
    assertFalse(HopDialogEditGuard.isReadOnly((IDialogEditable) () -> Permission.FILE_EDIT));
    assertFalse(HopDialogEditGuard.isReadOnly((IDialogEditable) () -> Permission.METADATA_WRITE));
  }

  @Test
  void nullPermissionMeansAlwaysEditable() {
    HopSecurity.setProvider(() -> HopSecurityContext.forUser("viewer", Set.of(HopRole.READ_ONLY)));
    IDialogEditable alwaysEditable = () -> null;
    assertFalse(HopDialogEditGuard.isReadOnly(alwaysEditable));
  }
}
