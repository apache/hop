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

package org.apache.hop.ui.core.widget.highlight;

import java.util.Objects;
import org.eclipse.swt.graphics.Color;

public class StyleAttribute {

  /** Foreground color */
  private Color foreground;

  /** The font style */
  private int style;

  /** Cached hash code. */
  private int hash;

  /**
   * Creates a text attribute with the given colors and style.
   *
   * @param foreground the foreground color, <code>null</code> if none
   * @param style the style, may be SWT.NORMAL, SWT.ITALIC or SWT.BOLD
   */
  public StyleAttribute(Color foreground, int style) {
    this.foreground = foreground;
    this.style = style;
  }

  @Override
  public boolean equals(Object object) {

    if (object == this) return true;

    if (!(object instanceof StyleAttribute)) return false;
    StyleAttribute a = (StyleAttribute) object;

    return (a.style == style && equals(a.foreground, foreground));
  }

  /**
   * Returns whether the two given objects are equal.
   *
   * @param o1 the first object, can be <code>null</code>
   * @param o2 the second object, can be <code>null</code>
   * @return <code>true</code> if the given objects are equals
   * @since 2.0
   */
  private boolean equals(Object o1, Object o2) {
    if (o1 != null) return o1.equals(o2);
    return (o2 == null);
  }

  @Override
  public int hashCode() {
    if (hash == 0) {
      hash = Objects.hash(foreground, style);
    }
    return hash;
  }

  /**
   * Returns the attribute's foreground color.
   *
   * @return the attribute's foreground color or <code>null</code> if not set
   */
  public Color getForeground() {
    return foreground;
  }

  /**
   * Returns the attribute's style.
   *
   * @return the attribute's style
   */
  public int getStyle() {
    return style;
  }
}
