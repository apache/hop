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

package org.apache.hop.core.gui.plugin;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.i18n.BaseMessages;

public class BaseGuiElements {

  protected String getTranslation(String string, String i18nPackage, Class<?> resourceClass ) {

    if ( StringUtils.isEmpty( string ) ) {
      return null;
    }
    if (string.startsWith( Const.I18N_PREFIX )) {
      String[] parts = string.split(":");
      if (parts.length == 3) {
        String alternativePackage = Const.NVL(parts[1], i18nPackage);
        String key = parts[2];
        return BaseMessages.getString( alternativePackage, key, resourceClass );
      }
    }

    String translation = BaseMessages.getString( i18nPackage, string, resourceClass );
    if (translation.startsWith( "!" ) && translation.endsWith( "!" )) {
      // Just return the original string, we did our best
      //
      return string;
    }
    return translation;
  }

  protected String calculateGetterMethod( GuiWidgetElement guiElement, String fieldName ) {
    if ( StringUtils.isNotEmpty( guiElement.getterMethod() ) ) {
      return guiElement.getterMethod();
    }
    String getter = "get" + StringUtil.initCap( fieldName );
    return getter;
  }


  protected String calculateSetterMethod( GuiWidgetElement guiElement, String fieldName ) {
    if ( StringUtils.isNotEmpty( guiElement.setterMethod() ) ) {
      return guiElement.setterMethod();
    }
    String getter = "set" + StringUtil.initCap( fieldName );
    return getter;
  }

}
