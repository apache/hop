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

package org.apache.hop.core.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;

public class TranslateUtil {

  public static String translate(String name, Class<?> parentObjectClass) {
    if (name.startsWith(Const.I18N_PREFIX)) {
      String[] parts = name.split(":");
      if (parts.length != 3) {
        return name;
      }

      String packageName = parts[1];
      String key = parts[2];

      String translation;
      if (StringUtils.isEmpty(packageName)) {
        translation = BaseMessages.getString(parentObjectClass, key);
      } else {
        translation = BaseMessages.getString(packageName, key);
      }
      return translation;
    } else {
      return name;
    }
  }
}
