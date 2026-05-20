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
package org.apache.hop.mail.workflow.actions.mailvalidator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class MailValidationTest {

  @Test
  void acceptsCommonValidAddresses() {
    assertTrue(MailValidation.isRegExValid("user@example.com"));
    assertTrue(MailValidation.isRegExValid("first.last+tag@sub.example.org"));
    assertTrue(MailValidation.isRegExValid("a1@b2.co"));
  }

  @Test
  void rejectsMalformedAddresses() {
    assertFalse(MailValidation.isRegExValid(""));
    assertFalse(MailValidation.isRegExValid("plainstring"));
    assertFalse(MailValidation.isRegExValid("no-at-sign.example.com"));
    assertFalse(MailValidation.isRegExValid("@no-local-part.example.com"));
    assertFalse(MailValidation.isRegExValid("user@"));
    assertFalse(MailValidation.isRegExValid("user@@double.example.com"));
  }
}
