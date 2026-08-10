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
package org.apache.hop.vfs.sftp.provider;

import com.jcraft.jsch.UserInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** Helper class to trust a new host. */
public class TrustEveryoneUserInfo implements UserInfo {
  private static final Log log = LogFactory.getLog(TrustEveryoneUserInfo.class);

  /** Constructs a new instance. */
  public TrustEveryoneUserInfo() {
    // empty
  }

  @Override
  public String getPassphrase() {
    return null;
  }

  @Override
  public String getPassword() {
    return null;
  }

  @Override
  public boolean promptPassphrase(final String s) {
    log.info(s + " - Answer: False");
    return false;
  }

  @Override
  public boolean promptPassword(final String s) {
    log.info(s + " - Answer: False");
    return false;
  }

  @Override
  public boolean promptYesNo(final String s) {
    log.debug(s + " - Answer: Yes");

    // trust
    return true;
  }

  @Override
  public void showMessage(final String s) {
    log.debug(s);
  }
}
