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

package org.apache.hop.core.gui;

import org.apache.hop.core.config.HopConfig;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/** How hops are drawn between transforms or actions on the canvas. */
public enum HopLineShape implements IEnumHasCodeAndDescription {
  /** A straight line from icon center to icon center. */
  STRAIGHT("HopLineShape.Straight"),
  /** A smooth curve that leaves and enters an icon on the side that faces the other icon. */
  CURVED("HopLineShape.Curved");

  /** The Hop GUI property that stores the selected shape. */
  public static final String GUI_PROPERTY_KEY = "HopLineShape";

  private final String descriptionKey;

  HopLineShape(String descriptionKey) {
    this.descriptionKey = descriptionKey;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getDescription() {
    return BaseMessages.getString(HopLineShape.class, descriptionKey);
  }

  public static String[] getDescriptions() {
    return IEnumHasCodeAndDescription.getDescriptions(HopLineShape.class);
  }

  public static HopLineShape lookupDescription(String description) {
    return IEnumHasCodeAndDescription.lookupDescription(HopLineShape.class, description, STRAIGHT);
  }

  public static HopLineShape lookupCode(String code) {
    return IEnumHasCode.lookupCode(HopLineShape.class, code, STRAIGHT);
  }

  /**
   * @return the shape selected in the Hop GUI options, or {@link #STRAIGHT} when none is set.
   */
  public static HopLineShape getConfigured() {
    try {
      return lookupCode(HopConfig.getGuiProperty(GUI_PROPERTY_KEY));
    } catch (Exception e) {
      return STRAIGHT;
    }
  }
}
