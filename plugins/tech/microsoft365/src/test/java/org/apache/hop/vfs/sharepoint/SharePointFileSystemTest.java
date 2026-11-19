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

package org.apache.hop.vfs.sharepoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.sharepoint.metadatatype.SharePointConnection;
import org.apache.hop.vfs.sharepoint.metadatatype.SharePointConnection.AuthenticationType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SharePointFileSystemTest {

  /** The credentials are logged, which needs the log store. */
  @BeforeAll
  static void initLogging() {
    HopLogStore.init();
  }

  @Test
  void rootHasTwoFixedFolders() {
    assertEquals(List.of("sites", "teams"), SharePointFileSystem.MANAGED_PATHS);
    assertTrue(SharePointFileSystem.isManagedPath("sites"));
    assertTrue(SharePointFileSystem.isManagedPath("Teams"));
    assertFalse(SharePointFileSystem.isManagedPath("personal"));
    assertFalse(SharePointFileSystem.isManagedPath(null));
  }

  @Test
  void siteIsListedUnderTheManagedPathOfItsAddress() {
    String sites = "https://contoso.sharepoint.com/sites/Finance";
    String teams = "https://contoso.sharepoint.com/teams/Finance";

    assertEquals("Finance", SharePointFileSystem.siteNameUnder("sites", sites));
    assertNull(SharePointFileSystem.siteNameUnder("teams", sites));
    assertEquals("Finance", SharePointFileSystem.siteNameUnder("teams", teams));
    assertNull(SharePointFileSystem.siteNameUnder("sites", teams));
  }

  @Test
  void siteNameIsTheDecodedLastPartOfTheAddress() {
    assertEquals(
        "Q1 report",
        SharePointFileSystem.siteNameUnder(
            "sites", "https://contoso.sharepoint.com/Sites/Q1%20report/"));
  }

  @Test
  void rootSiteAndSubsitesAreNotListed() {
    assertNull(SharePointFileSystem.siteNameUnder("sites", "https://contoso.sharepoint.com"));
    assertNull(SharePointFileSystem.siteNameUnder("sites", "https://contoso.sharepoint.com/"));
    assertNull(SharePointFileSystem.siteNameUnder("sites", "https://contoso.sharepoint.com/sites"));
    assertNull(
        SharePointFileSystem.siteNameUnder(
            "sites", "https://contoso.sharepoint.com/sites/Finance/Audit"));
    assertNull(SharePointFileSystem.siteNameUnder("sites", null));
  }

  @Test
  void emptyCertificateIsReported() {
    SharePointConnection connection = new SharePointConnection();
    connection.setAuthenticationType(AuthenticationType.CERTIFICATE);
    connection.setTenantId("tenant");
    connection.setClientId("client");

    HopException e =
        assertThrows(
            HopException.class, () -> connection.credentials(LogChannel.GENERAL, new Variables()));
    assertTrue(e.getMessage().contains("PEM Certificate"), e.getMessage());
  }

  @Test
  void emptyTenantIsReported() {
    SharePointConnection connection = new SharePointConnection();
    connection.setClientId("client");
    connection.setClientSecret("secret");

    assertThrows(
        HopException.class, () -> connection.credentials(LogChannel.GENERAL, new Variables()));
  }
}
