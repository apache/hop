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

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/**
 * Whether an FTPS connection encrypts its data connections as well as its commands.
 *
 * <p>FTP carries the commands and the file contents over two separate connections. Turning on FTPS
 * encrypts the command connection; this is the separate decision about the data connection, sent to
 * the server as the argument of {@code PROT}.
 *
 * <p>The codes are the single letters of that command, which is what gets stored and what an
 * administrator reading an FTP manual will recognise. The descriptions are what the user picks
 * from.
 */
public enum FtpDataChannelProtection implements IEnumHasCodeAndDescription {
  /** The data connection is encrypted too. What you want unless something forbids it. */
  PRIVATE("P"),

  /**
   * The data connection is plain TCP. The login stays protected because it travels over the command
   * connection, but the file contents go over the wire in the clear.
   */
  CLEAR("C"),
  ;

  private final String code;

  FtpDataChannelProtection(String code) {
    this.code = code;
  }

  @Override
  public String getCode() {
    return code;
  }

  /** Looked up per call: the language of the UI is picked after this class is loaded. */
  @Override
  public String getDescription() {
    return BaseMessages.getString(
        FtpDataChannelProtection.class, "FtpDataChannelProtection." + name() + ".Label");
  }

  public static String[] getDescriptions() {
    return IEnumHasCodeAndDescription.getDescriptions(FtpDataChannelProtection.class);
  }

  public static FtpDataChannelProtection lookupDescription(String description) {
    return IEnumHasCodeAndDescription.lookupDescription(
        FtpDataChannelProtection.class, description, PRIVATE);
  }

  /** Anything unrecognised reads as {@link #PRIVATE}: the safe end of the choice. */
  public static FtpDataChannelProtection lookupCode(String code) {
    return IEnumHasCode.lookupCode(FtpDataChannelProtection.class, code, PRIVATE);
  }
}
