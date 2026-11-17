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

package org.apache.hop.pipeline.transforms.xml.xmljoin;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/**
 * Where the XML join adds the source elements. Only shown in the dialog: the transform stores it as
 * {@link XmlJoinMeta#isComplexJoin()}.
 */
public enum XmlJoinMode implements IEnumHasCodeAndDescription {
  /** Every element goes to the one node the XPath statement finds. */
  ONE_NODE("XmlJoinMode.OneNode.Description"),

  /** The key field of each row replaces ? in the XPath statement: a complex join. */
  BY_KEY("XmlJoinMode.ByKey.Description");

  private final String descriptionKey;

  XmlJoinMode(String descriptionKey) {
    this.descriptionKey = descriptionKey;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getDescription() {
    return BaseMessages.getString(XmlJoinMeta.class, descriptionKey);
  }
}
