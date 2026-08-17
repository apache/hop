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

package org.apache.hop.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.awt.image.BufferedImage;
import java.lang.reflect.Method;
import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.ImageData;
import org.junit.jupiter.api.Test;

/** Unit test for {@link SwtUniversalImage} */
class SwtUniversalImageTest {

  @Test
  void pixelSizeScalesLinearlyWithZoom() {
    assertEquals(16, SwtUniversalImage.pixelSize(16, 100));
    assertEquals(32, SwtUniversalImage.pixelSize(16, 200));
    assertEquals(24, SwtUniversalImage.pixelSize(16, 150));
    assertEquals(1, SwtUniversalImage.pixelSize(0, 200));
  }

  @Test
  void toImageDataKeepsSizeAndAlpha() {
    BufferedImage src = new BufferedImage(16, 8, BufferedImage.TYPE_INT_ARGB);
    src.setRGB(0, 0, 0x80FF0000);
    ImageData data = SwtUniversalImage.toImageData(src);

    assertEquals(16, data.width);
    assertEquals(8, data.height);
    assertEquals(0x80, data.getAlpha(0, 0));
  }

  @Test
  void createDpiAwareImageSignatureDoesNotReferenceDesktopImageDataProvider() throws Exception {
    Method method =
        SwtUniversalImage.class.getMethod(
            "createDpiAwareImage", Device.class, SwtUniversalImage.ImageDataAtZoom.class);
    for (Class<?> type : method.getParameterTypes()) {
      assertFalse(
          type.getName().contains("ImageDataProvider"),
          "RAP class loading resolves method signatures; keep ImageDataProvider off this API");
    }
  }
}
