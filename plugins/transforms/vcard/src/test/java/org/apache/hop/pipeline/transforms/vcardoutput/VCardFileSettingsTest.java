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
package org.apache.hop.pipeline.transforms.vcardoutput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;

class VCardFileSettingsTest {

  @Test
  void buildFilenameAddsExtensionAndDatePlaceholders() {
    VCardFileSettings settings = new VCardFileSettings();
    settings.setFileName("/tmp/contact");
    settings.setExtension("vcf");
    settings.setDateInFileName(true);
    settings.setTimeInFileName(true);

    String filename = settings.buildFilename(new Variables(), false);
    assertEquals("/tmp/contact_<yyyyMMdd>_<HHmmss>.vcf", filename);
  }

  @Test
  void getFilesReturnsSampleNames() {
    VCardFileSettings settings = new VCardFileSettings();
    settings.setFileName("/tmp/contact");
    settings.setExtension("vcf");

    assertEquals(2, settings.getFiles(new Variables(), true).length);
  }

  /** The file that is written carries the date and time, not the placeholders. */
  @Test
  void buildFilenameForADateUsesItsDateAndTime() {
    VCardFileSettings settings = new VCardFileSettings();
    settings.setFileName("/tmp/contact");
    settings.setExtension("vcf");
    settings.setDateInFileName(true);
    settings.setTimeInFileName(true);
    Date date = new GregorianCalendar(2026, Calendar.MARCH, 9, 14, 5, 7).getTime();

    assertEquals("/tmp/contact_20260309_140507.vcf", settings.buildFilename(new Variables(), date));

    settings.setDateInFileName(false);
    assertEquals("/tmp/contact_140507.vcf", settings.buildFilename(new Variables(), date));

    settings.setTimeInFileName(false);
    settings.setExtension("");
    assertEquals("/tmp/contact", settings.buildFilename(new Variables(), date));
  }

  @Test
  void theSampleNameHasNoPlaceholders() {
    VCardFileSettings settings = new VCardFileSettings();
    settings.setFileName("/tmp/contact");
    settings.setDateInFileName(true);
    settings.setTimeInFileName(true);

    assertTrue(
        settings.buildFilename(new Variables(), true).matches("/tmp/contact_\\d{8}_\\d{6}\\.vcf"));
  }
}
