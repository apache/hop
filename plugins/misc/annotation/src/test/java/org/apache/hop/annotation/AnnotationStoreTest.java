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

package org.apache.hop.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.hop.core.attributes.AttributesUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.junit.jupiter.api.Test;

class AnnotationStoreTest {

  @Test
  void roundTripOnATransform() {
    TransformMeta transform = new TransformMeta("Dummy", "read", new DummyMeta());
    Annotation annotation = annotation("Start here", AnnotationPlacement.TOP_RIGHT);

    AnnotationStore.write(transform, annotation);

    Annotation read = AnnotationStore.read(transform);
    assertNotNull(read);
    assertEquals("Start here", read.getText());
    assertEquals(AnnotationPlacement.TOP_RIGHT, read.getPlacement());
    assertTrue(read.isDashedLine());
    assertEquals(
        "Y", transform.getAttribute(AnnotationStore.GROUP, AnnotationStore.KEY_DASHED_LINE));
    String xml = AttributesUtil.getAttributesXml(transform.getAttributesMap());
    assertTrue(xml.contains("annotation"));
    assertTrue(xml.contains("Start here"));
    assertTrue(xml.contains("TOP_RIGHT"));
  }

  @Test
  void roundTripOnAnActionAndUnknownPlacementFallsBackToTop() {
    ActionMeta action = new ActionMeta(new ActionDummy("wait"));
    Annotation annotation = annotation("Wait for the file", AnnotationPlacement.LEFT);
    AnnotationStore.write(action, annotation);

    Annotation read = AnnotationStore.read(action);
    assertEquals("Wait for the file", read.getText());
    assertEquals(AnnotationPlacement.LEFT, read.getPlacement());

    action.setAttribute(AnnotationStore.GROUP, AnnotationStore.KEY_PLACEMENT, "beside");
    assertEquals(AnnotationPlacement.TOP, AnnotationStore.read(action).getPlacement());
  }

  @Test
  void dashedLineIsStoredAndAMissingKeyStaysConnected() {
    TransformMeta transform = new TransformMeta();
    Annotation annotation = annotation("Start", AnnotationPlacement.TOP);
    annotation.setDashedLine(false);
    AnnotationStore.write(transform, annotation);

    assertFalse(AnnotationStore.read(transform).isDashedLine());
    assertEquals(
        "N", transform.getAttribute(AnnotationStore.GROUP, AnnotationStore.KEY_DASHED_LINE));

    transform.getAttributes(AnnotationStore.GROUP).remove(AnnotationStore.KEY_DASHED_LINE);
    assertTrue(AnnotationStore.read(transform).isDashedLine());
  }

  @Test
  void blankTextClearsTheGroup() {
    TransformMeta transform = new TransformMeta();
    AnnotationStore.write(transform, annotation("Keep", AnnotationPlacement.BOTTOM));
    assertNotNull(AnnotationStore.read(transform));

    AnnotationStore.write(transform, annotation("   ", AnnotationPlacement.BOTTOM));
    assertNull(AnnotationStore.read(transform));
    assertNull(transform.getAttributes(AnnotationStore.GROUP));

    AnnotationStore.write(transform, annotation("Again", AnnotationPlacement.TOP));
    AnnotationStore.clear(transform);
    assertNull(AnnotationStore.read(transform));
  }

  @Test
  void writeReplacesTheGroupMapAShallowCloneStillHolds() {
    ActionMeta original = new ActionMeta(new ActionDummy("start"));
    AnnotationStore.write(original, annotation("Start", AnnotationPlacement.RIGHT));
    Map<String, String> shared = original.getAttributes(AnnotationStore.GROUP);

    ActionMeta shallow = new ActionMeta(new ActionDummy("start-copy"));
    shallow.getAttributesMap().put(AnnotationStore.GROUP, shared);

    AnnotationStore.write(original, annotation("End", AnnotationPlacement.LEFT));

    assertEquals("Start", shared.get(AnnotationStore.KEY_TEXT));
    assertEquals("End", original.getAttribute(AnnotationStore.GROUP, AnnotationStore.KEY_TEXT));
    assertEquals("Start", shallow.getAttribute(AnnotationStore.GROUP, AnnotationStore.KEY_TEXT));
    assertNotSame(
        original.getAttributes(AnnotationStore.GROUP),
        shallow.getAttributes(AnnotationStore.GROUP));
  }

  @Test
  void transformCloneKeepsTheAnnotationItHad() throws Exception {
    TransformMeta original = new TransformMeta("Dummy", "read", new DummyMeta());
    AnnotationStore.write(original, annotation("Start", AnnotationPlacement.TOP));

    TransformMeta cloned = (TransformMeta) original.clone();
    AnnotationStore.write(original, annotation("Moved", AnnotationPlacement.BOTTOM));

    assertEquals("Moved", AnnotationStore.read(original).getText());
    assertEquals(AnnotationPlacement.TOP, AnnotationStore.read(cloned).getPlacement());
    assertEquals("Start", AnnotationStore.read(cloned).getText());
  }

  private static Annotation annotation(String text, AnnotationPlacement placement) {
    Annotation annotation = new Annotation();
    annotation.setText(text);
    annotation.setPlacement(placement);
    return annotation;
  }
}
