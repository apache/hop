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

package org.apache.hop.core.gui.markdown;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.imageio.ImageIO;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NoteImageSupportTest {

  @AfterEach
  void clearCache() {
    NoteImageSupport.clearCache();
  }

  @Test
  void rejectsNetworkAndScriptTargets() {
    assertFalse(NoteImageSupport.isAllowedImageTarget("https://evil.example/a.png"));
    assertFalse(NoteImageSupport.isAllowedImageTarget("http://x"));
    assertFalse(NoteImageSupport.isAllowedImageTarget("javascript:alert(1)"));
    assertFalse(NoteImageSupport.isAllowedImageTarget("data:image/png;base64,xx"));
    assertTrue(NoteImageSupport.isAllowedImageTarget("images/logo.png"));
    assertTrue(NoteImageSupport.isAllowedImageTarget("/tmp/a.png"));
  }

  @Test
  void resolvePathRelativeToBase(@TempDir Path dir) throws Exception {
    Path base = dir.resolve("pipeline.hpl");
    Files.writeString(base, "<pipeline/>");
    Path img = dir.resolve("pic.png");
    writePng(img, 40, 20);

    String resolved =
        NoteImageSupport.resolvePath(new Variables(), base.toAbsolutePath().toString(), "pic.png");
    assertNotNull(resolved);
    assertTrue(resolved.contains("pic.png"));
  }

  @Test
  void resolvePathRejectsHttp() throws Exception {
    assertNull(NoteImageSupport.resolvePath(new Variables(), "/tmp/a.hpl", "https://x/y.png"));
  }

  @Test
  void probeReadsRasterDimensions(@TempDir Path dir) throws Exception {
    Path img = dir.resolve("box.png");
    writePng(img, 80, 40);
    NoteImageSupport.ImageInfo info = NoteImageSupport.probe(img.toAbsolutePath().toString());
    assertTrue(info.available());
    assertEquals(80, info.width());
    assertEquals(40, info.height());
    assertFalse(info.svg());
  }

  @Test
  void fitPreservesAspectAndDoesNotUpscale() {
    Point fitted = NoteImageSupport.fit(100, 50, 200, 200);
    assertEquals(100, fitted.x);
    assertEquals(50, fitted.y);
    Point shrink = NoteImageSupport.fit(400, 200, 100, 100);
    assertEquals(100, shrink.x);
    assertEquals(50, shrink.y);
  }

  private static void writePng(Path path, int w, int h) throws Exception {
    BufferedImage image = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
    Graphics2D g = image.createGraphics();
    g.setColor(Color.BLUE);
    g.fillRect(0, 0, w, h);
    g.dispose();
    ImageIO.write(image, "png", path.toFile());
  }
}
