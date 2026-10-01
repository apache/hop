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

package org.apache.hop.vfs.ftp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * The security mode is stored by code and shown by description, so both have to keep working - a
 * changed code silently breaks every connection already in a project.
 */
class FtpSecurityModeTest {

  @Test
  @DisplayName("The stored codes are the ones already in the metadata of existing projects")
  void codesAreStable() {
    assertEquals("FTP", FtpSecurityMode.FTP.getCode());
    assertEquals("FTPS_EXPLICIT", FtpSecurityMode.FTPS_EXPLICIT.getCode());
    assertEquals("FTPS_IMPLICIT", FtpSecurityMode.FTPS_IMPLICIT.getCode());
  }

  @ParameterizedTest
  @EnumSource(FtpSecurityMode.class)
  @DisplayName("Every mode round-trips through its code")
  void everyModeRoundTripsThroughItsCode(FtpSecurityMode mode) {
    assertSame(mode, FtpSecurityMode.lookupCode(mode.getCode()));
  }

  @ParameterizedTest
  @EnumSource(FtpSecurityMode.class)
  @DisplayName("Every mode round-trips through its description")
  void everyModeRoundTripsThroughItsDescription(FtpSecurityMode mode) {
    assertSame(mode, FtpSecurityMode.lookupDescription(mode.getDescription()));
  }

  @Test
  @DisplayName("Something unrecognised reads as plain FTP rather than blowing up")
  void unknownValuesFallBackToPlainFtp() {
    assertSame(FtpSecurityMode.FTP, FtpSecurityMode.lookupCode("SOMETHING_ELSE"));
    assertSame(FtpSecurityMode.FTP, FtpSecurityMode.lookupCode(""));
    assertSame(FtpSecurityMode.FTP, FtpSecurityMode.lookupDescription("something else"));
  }

  @Test
  @DisplayName("The descriptions are distinct, so the combo can be read back")
  void descriptionsAreDistinct() {
    String[] descriptions = FtpSecurityMode.getDescriptions();

    assertEquals(FtpSecurityMode.values().length, descriptions.length);
    assertEquals(
        descriptions.length,
        Arrays.stream(descriptions).distinct().count(),
        "two modes with the same description can't be told apart: "
            + Arrays.toString(descriptions));
    Arrays.stream(descriptions)
        .forEach(d -> assertFalse(d.startsWith("!"), "missing translation: " + d));
  }

  @Test
  @DisplayName("Only the FTPS modes are secure, and only they use the ftps scheme")
  void schemesAndSecurity() {
    assertFalse(FtpSecurityMode.FTP.isSecure());
    assertTrue(FtpSecurityMode.FTPS_EXPLICIT.isSecure());
    assertTrue(FtpSecurityMode.FTPS_IMPLICIT.isSecure());

    assertEquals("ftp", FtpSecurityMode.FTP.getScheme());
    assertEquals("ftps", FtpSecurityMode.FTPS_EXPLICIT.getScheme());
    assertEquals("ftps", FtpSecurityMode.FTPS_IMPLICIT.getScheme());
  }

  @Test
  @DisplayName("The data channel protection stores the PROT letter and shows words")
  void dataChannelProtectionCodesAreTheProtocolLetters() {
    assertEquals("P", FtpDataChannelProtection.PRIVATE.getCode());
    assertEquals("C", FtpDataChannelProtection.CLEAR.getCode());

    for (FtpDataChannelProtection protection : FtpDataChannelProtection.values()) {
      assertSame(protection, FtpDataChannelProtection.lookupCode(protection.getCode()));
      assertSame(
          protection, FtpDataChannelProtection.lookupDescription(protection.getDescription()));
      assertFalse(
          protection.getDescription().startsWith("!"),
          "missing translation: " + protection.getDescription());
      assertFalse(
          protection.getDescription().equals(protection.getCode()),
          "the user should see words, not the bare protocol letter");
    }
  }

  @Test
  @DisplayName("Anything unrecognised reads as the encrypted choice, the safe end")
  void unknownProtectionFallsBackToPrivate() {
    assertSame(FtpDataChannelProtection.PRIVATE, FtpDataChannelProtection.lookupCode("Z"));
    assertSame(FtpDataChannelProtection.PRIVATE, FtpDataChannelProtection.lookupDescription("?"));
    assertEquals(
        FtpDataChannelProtection.values().length,
        FtpDataChannelProtection.getDescriptions().length);
  }

  @Test
  @DisplayName("Only implicit FTPS has a port of its own")
  void defaultPorts() {
    assertEquals(21, FtpSecurityMode.FTP.getDefaultPort());
    assertEquals(21, FtpSecurityMode.FTPS_EXPLICIT.getDefaultPort());
    assertEquals(990, FtpSecurityMode.FTPS_IMPLICIT.getDefaultPort());
  }
}
