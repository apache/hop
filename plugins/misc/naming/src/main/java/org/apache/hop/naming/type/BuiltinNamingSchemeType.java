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

package org.apache.hop.naming.type;

import org.apache.hop.core.naming.INamingSchemeType;
import org.apache.hop.core.naming.NamingSchemeTypePlugin;

/** Shared implementation: code and label come from {@link NamingSchemeTypePlugin}. */
public abstract class BuiltinNamingSchemeType implements INamingSchemeType {

  @Override
  public String getCode() {
    NamingSchemeTypePlugin plugin = getClass().getAnnotation(NamingSchemeTypePlugin.class);
    return plugin != null ? plugin.id() : "";
  }

  @Override
  public String getName() {
    NamingSchemeTypePlugin plugin = getClass().getAnnotation(NamingSchemeTypePlugin.class);
    return plugin != null ? plugin.name() : getCode();
  }
}
