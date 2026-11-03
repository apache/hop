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

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.IAttributes;

/** Reads and writes one annotation on a transform or action attribute map. */
public final class AnnotationStore {
  public static final String GROUP = "annotation";
  static final String KEY_TEXT = "text";
  static final String KEY_PLACEMENT = "placement";
  static final String KEY_DASHED_LINE = "dashed-line";

  private AnnotationStore() {}

  /**
   * @param holder the transform or action, or null
   * @return the annotation, or null when there is none or the text is blank
   */
  public static Annotation read(IAttributes holder) {
    if (holder == null) {
      return null;
    }
    Map<String, String> group = holder.getAttributes(GROUP);
    if (group == null) {
      return null;
    }
    String text = group.get(KEY_TEXT);
    if (isBlank(text)) {
      return null;
    }
    Annotation annotation = new Annotation();
    annotation.setText(text);
    annotation.setPlacement(AnnotationPlacement.fromCode(group.get(KEY_PLACEMENT)));
    // A missing key stays connected. Annotations saved before the option existed get the line.
    annotation.setDashedLine(!"N".equalsIgnoreCase(group.get(KEY_DASHED_LINE)));
    return annotation;
  }

  /**
   * Stores the annotation on a new group map. A blank text removes the group. Replacing the map
   * leaves a shallow clone, such as {@code ActionMeta.clone()}, holding the previous values.
   *
   * @param holder the transform or action
   * @param annotation the annotation to store, or null to remove it
   */
  public static void write(IAttributes holder, Annotation annotation) {
    if (holder == null) {
      return;
    }
    if (annotation == null || isBlank(annotation.getText())) {
      clear(holder);
      return;
    }
    AnnotationPlacement placement =
        annotation.getPlacement() == null ? AnnotationPlacement.TOP : annotation.getPlacement();
    Map<String, String> group = new HashMap<>();
    group.put(KEY_TEXT, annotation.getText());
    group.put(KEY_PLACEMENT, placement.getCode());
    group.put(KEY_DASHED_LINE, annotation.isDashedLine() ? "Y" : "N");
    holder.setAttributes(GROUP, group);
  }

  /**
   * Removes the annotation group.
   *
   * @param holder the transform or action
   */
  public static void clear(IAttributes holder) {
    if (holder == null) {
      return;
    }
    Map<String, Map<String, String>> attributes = holder.getAttributesMap();
    if (attributes != null) {
      attributes.remove(GROUP);
    }
  }

  private static boolean isBlank(String text) {
    return text == null || text.isBlank();
  }
}
