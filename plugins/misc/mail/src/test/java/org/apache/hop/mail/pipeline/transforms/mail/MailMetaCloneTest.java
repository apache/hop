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
package org.apache.hop.mail.pipeline.transforms.mail;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.mail.workflow.actions.mail.MailEmbeddedImageField;
import org.junit.jupiter.api.Test;

class MailMetaCloneTest {

  @Test
  void cloneCopiesScalarFields() {
    MailMeta original = populated();

    MailMeta copy = (MailMeta) original.clone();

    assertInstanceOf(MailMeta.class, copy);
    assertNotSame(original, copy);
    assertScalarFieldsEqual(original, copy);
  }

  @Test
  void cloneDeepCopiesEmbeddedImagesList() {
    MailMeta original = populated();
    MailEmbeddedImageField img = new MailEmbeddedImageField();
    img.setContentId("cid-1");
    img.setEmbeddedImage("/tmp/img.png");
    original.embeddedImages.add(img);

    MailMeta copy = (MailMeta) original.clone();

    assertEquals(1, copy.embeddedImages.size());
    assertNotSame(original.embeddedImages, copy.embeddedImages, "list itself must not be shared");
    assertNotSame(
        original.embeddedImages.get(0),
        copy.embeddedImages.get(0),
        "MailEmbeddedImageField must be deep-copied");
    assertEquals("cid-1", copy.embeddedImages.get(0).getContentId());
    assertEquals("/tmp/img.png", copy.embeddedImages.get(0).getEmbeddedImage());
  }

  @Test
  void mutatingCloneDoesNotAffectOriginal() {
    MailMeta original = populated();
    MailMeta copy = (MailMeta) original.clone();

    copy.setServer("smtp.different.example.com");
    copy.setSubject("changed");

    assertEquals("smtp.example.com", original.getServer());
    assertEquals("original subject", original.getSubject());
  }

  @Test
  void mutatingCloneEmbeddedImagesListDoesNotAffectOriginal() {
    MailMeta original = populated();
    MailMeta copy = (MailMeta) original.clone();

    copy.embeddedImages.add(new MailEmbeddedImageField());

    assertTrue(original.embeddedImages.isEmpty());
    assertEquals(1, copy.embeddedImages.size());
  }

  /** Build a MailMeta with a sampling of fields filled in for clone-comparison purposes. */
  private static MailMeta populated() {
    MailMeta meta = new MailMeta();
    meta.setServer("smtp.example.com");
    meta.setDestination("to@example.com");
    meta.setDestinationCc("cc@example.com");
    meta.setDestinationBCc("bcc@example.com");
    meta.setReplyAddress("reply@example.com");
    meta.setReplyName("Bot");
    meta.setSubject("original subject");
    meta.setIncludeDate(true);
    meta.setContactPerson("Alice");
    meta.setContactPhone("+1");
    meta.setComment("hello");
    meta.setIncludingFiles(true);
    meta.setZipFiles(true);
    meta.setZipFilename("archive.zip");
    meta.setUsingAuthentication(true);
    meta.setAuthenticationUser("alice");
    meta.setAuthenticationPassword("secret");
    meta.setUseHTML(true);
    meta.setUsingSecureAuthentication(true);
    meta.setUsePriority(true);
    meta.setPort("587");
    meta.setPriority("high");
    meta.setImportance("high");
    meta.setSensitivity("normal");
    meta.setSecureConnectionType("TLS");
    meta.setEncoding("UTF-8");
    meta.setReplyToAddresses("reply2@example.com");
    return meta;
  }

  private static void assertScalarFieldsEqual(MailMeta a, MailMeta b) {
    assertEquals(a.getServer(), b.getServer());
    assertEquals(a.getDestination(), b.getDestination());
    assertEquals(a.getDestinationCc(), b.getDestinationCc());
    assertEquals(a.getDestinationBCc(), b.getDestinationBCc());
    assertEquals(a.getReplyAddress(), b.getReplyAddress());
    assertEquals(a.getReplyName(), b.getReplyName());
    assertEquals(a.getSubject(), b.getSubject());
    assertEquals(a.isIncludeDate(), b.isIncludeDate());
    assertEquals(a.getContactPerson(), b.getContactPerson());
    assertEquals(a.getContactPhone(), b.getContactPhone());
    assertEquals(a.getComment(), b.getComment());
    assertEquals(a.isIncludingFiles(), b.isIncludingFiles());
    assertEquals(a.isZipFiles(), b.isZipFiles());
    assertEquals(a.getZipFilename(), b.getZipFilename());
    assertEquals(a.isUsingAuthentication(), b.isUsingAuthentication());
    assertEquals(a.getAuthenticationUser(), b.getAuthenticationUser());
    assertEquals(a.getAuthenticationPassword(), b.getAuthenticationPassword());
    assertEquals(a.isUseHTML(), b.isUseHTML());
    assertEquals(a.isUsingSecureAuthentication(), b.isUsingSecureAuthentication());
    assertEquals(a.isUsePriority(), b.isUsePriority());
    assertEquals(a.getPort(), b.getPort());
    assertEquals(a.getPriority(), b.getPriority());
    assertEquals(a.getImportance(), b.getImportance());
    assertEquals(a.getSensitivity(), b.getSensitivity());
    assertEquals(a.getSecureConnectionType(), b.getSecureConnectionType());
    assertEquals(a.getEncoding(), b.getEncoding());
    assertEquals(a.getReplyToAddresses(), b.getReplyToAddresses());
  }
}
