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

package org.apache.hop.mail.common;

import lombok.experimental.UtilityClass;

/** mail const */
@UtilityClass
public class MailConst {
  public static final String MAIL_PREFIX = "mail.";

  /** mail support protocol */
  public static final String INBOX_FOLDER = "INBOX";

  public static final String PROTOCOL_STRING_IMAP = "IMAP";
  public static final String PROTOCOL_STRING_POP3 = "POP3";

  public static final String PROTOCOL_MBOX = "MBOX";

  public static final String PROTOCOL_SMTP = "SMTP";
  public static final String PROTOCOL_SSL_SMTP = "smtps";

  public static final String PROTOCOL_MSTOR = "mstor";

  public static final String SSL_TLS = "TLS";
  public static final String SSL_TLS_12 = "TLS 1.2";
  public static final String SSL_TLS_V12 = "TLSv1.2";
}
