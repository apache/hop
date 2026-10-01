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

package org.apache.hop.ui.core.widget.svg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.awt.Dimension;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.batik.anim.dom.SAXSVGDocumentFactory;
import org.apache.batik.util.XMLResourceDescriptor;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.service.ResourceManager;
import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.ImageData;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.internal.graphics.ImageFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

class SvgImageFacadeImplTest {

  private static final String ICON =
      "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"24\" height=\"24\">"
          + "<path fill=\"#0e3a5a\" d=\"M2 2h20v20H2z\"/></svg>";

  private MockedStatic<RWT> rwt;
  private ResourceManager resourceManager;
  private final SvgImageFacadeImpl facade = new SvgImageFacadeImpl();
  private final Device device = mock(Device.class);

  /** What the mock resource manager has been handed, so it can serve it back like RWT does. */
  private final Map<String, byte[]> registered = new HashMap<>();

  @BeforeEach
  void mockRwt() throws Exception {
    resourceManager = mock(ResourceManager.class);
    when(resourceManager.getLocation(anyString()))
        .thenAnswer(invocation -> "rwt-resources/" + invocation.getArgument(0));
    doAnswer(
            invocation -> {
              InputStream stream = invocation.getArgument(1);
              registered.put(invocation.getArgument(0), stream.readAllBytes());
              return null;
            })
        .when(resourceManager)
        .register(anyString(), any(InputStream.class));
    when(resourceManager.getRegisteredContent(anyString()))
        .thenAnswer(
            invocation -> {
              byte[] bytes = registered.get(invocation.<String>getArgument(0));
              return bytes == null ? null : new ByteArrayInputStream(bytes);
            });
    rwt = mockStatic(RWT.class);
    rwt.when(RWT::getResourceManager).thenReturn(resourceManager);
  }

  @AfterEach
  void closeRwt() {
    rwt.close();
  }

  private static Document parse(String xml) throws Exception {
    return new SAXSVGDocumentFactory(XMLResourceDescriptor.getXMLParserClassName())
        .createDocument("icon.svg", new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void publishesTheSizedSvgAndPointsALogicallySizedImageAtIt() throws Exception {
    Image image = facade.createImageInternal(device, parse(ICON), new Dimension(24, 24), 16, 16);

    assertNotNull(image);
    assertEquals(new Rectangle(0, 0, 16, 16), image.getBounds(), "tree cells stay 16px wide");

    ArgumentCaptor<String> name = ArgumentCaptor.forClass(String.class);
    verify(resourceManager).register(name.capture(), any(InputStream.class));
    assertTrue(name.getValue().startsWith(SvgImageFacadeImpl.RESOURCE_PREFIX));
    assertTrue(name.getValue().endsWith(".svg"));
    assertEquals("rwt-resources/" + name.getValue(), ImageFactory.getImagePath(image));

    String svg = new String(registered.get(name.getValue()), StandardCharsets.UTF_8);
    assertTrue(svg.contains("width=\"16\""), svg);
    assertTrue(svg.contains("height=\"16\""), svg);
    assertTrue(svg.contains("viewBox=\"0 0 24 24\""), svg);
  }

  @Test
  void sameSvgAndSizeReuseTheRegisteredResource() throws Exception {
    Image first = facade.createImageInternal(device, parse(ICON), new Dimension(24, 24), 16, 16);
    when(resourceManager.isRegistered(anyString())).thenReturn(true);

    Image second = facade.createImageInternal(device, parse(ICON), new Dimension(24, 24), 16, 16);

    verify(resourceManager).register(anyString(), any(InputStream.class));
    assertEquals(ImageFactory.getImagePath(first), ImageFactory.getImagePath(second));
  }

  @Test
  void differentSizeOrContentGetsItsOwnResource() throws Exception {
    Image small = facade.createImageInternal(device, parse(ICON), new Dimension(24, 24), 16, 16);
    Image large = facade.createImageInternal(device, parse(ICON), new Dimension(24, 24), 32, 32);
    Image other =
        facade.createImageInternal(
            device, parse(ICON.replace("#0e3a5a", "#c8e7fa")), new Dimension(24, 24), 16, 16);

    String smallPath = ImageFactory.getImagePath(small);
    assertNotEquals(smallPath, ImageFactory.getImagePath(large));
    assertNotEquals(smallPath, ImageFactory.getImagePath(other));
  }

  @Test
  void vectorImagesCanStillBeRasterisedAtAnyZoom() throws Exception {
    Image image = facade.createImageInternal(device, parse(ICON), new Dimension(24, 24), 16, 16);

    ImageData at100 = facade.rasterizeInternal(image, 100);
    ImageData at200 = facade.rasterizeInternal(image, 200);

    assertEquals(16, at100.width);
    assertEquals(16, at100.height);
    assertEquals(32, at200.width);
    // The square path covers the centre with the icon's fill colour.
    assertEquals(new RGB(0x0e, 0x3a, 0x5a), at100.palette.getRGB(at100.getPixel(8, 8)));
    assertEquals(255, at200.getAlpha(16, 16));
    assertEquals(0, at100.getAlpha(0, 0), "outside the path stays transparent");
  }

  @Test
  void rasterisingAnImageThatIsNotVectorBackedIsNotOurBusiness() {
    Image foreign = mock(Image.class);
    assertNull(facade.rasterizeInternal(foreign, 100));
  }

  @Test
  void overlayNestsTheBadgeInTheBottomRightCornerOfTheBase() throws Exception {
    Image base = facade.createImageInternal(device, parse(ICON), new Dimension(24, 24), 16, 16);
    Image badge =
        facade.createImageInternal(
            device, parse(ICON.replace("#0e3a5a", "#ff0000")), new Dimension(24, 24), 8, 8);

    Image badged = facade.overlayInternal(device, base, badge, 8, 1);

    assertNotNull(badged);
    assertEquals(new Rectangle(0, 0, 16, 16), badged.getBounds());
    String name = ImageFactory.getImagePath(badged).substring("rwt-resources/".length());
    Element root =
        parse(new String(registered.get(name), StandardCharsets.UTF_8)).getDocumentElement();
    NodeList nested = root.getElementsByTagName("svg");
    assertEquals(2, nested.getLength(), "base and badge as nested <svg>");
    Element nestedBase = (Element) nested.item(0);
    assertEquals("16", nestedBase.getAttribute("width"));
    assertEquals("0 0 24 24", nestedBase.getAttribute("viewBox"), "keeps its own scaling");
    Element nestedBadge = (Element) nested.item(1);
    assertEquals("7", nestedBadge.getAttribute("x"));
    assertEquals("7", nestedBadge.getAttribute("y"));
    assertEquals("8", nestedBadge.getAttribute("width"));
    assertEquals("8", nestedBadge.getAttribute("height"));

    ImageData pixels = facade.rasterizeInternal(badged, 100);
    assertEquals(new RGB(0xff, 0, 0), pixels.palette.getRGB(pixels.getPixel(11, 11)), "badge");
    assertEquals(new RGB(0x0e, 0x3a, 0x5a), pixels.palette.getRGB(pixels.getPixel(3, 3)), "base");
  }

  @Test
  void overlayNeedsBothImagesToBeVectorBacked() throws Exception {
    Image base = facade.createImageInternal(device, parse(ICON), new Dimension(24, 24), 16, 16);
    assertNull(facade.overlayInternal(device, base, mock(Image.class), 8, 1));
    assertNull(facade.overlayInternal(device, mock(Image.class), base, 8, 1));
  }

  @Test
  void withoutARapContextTheCallerRasterises() throws Exception {
    rwt.when(RWT::getResourceManager).thenThrow(new IllegalStateException("No context"));

    Image image = facade.createImageInternal(device, parse(ICON), new Dimension(24, 24), 16, 16);

    assertNull(image);
    verify(resourceManager, never()).register(anyString(), any(InputStream.class));
  }
}
