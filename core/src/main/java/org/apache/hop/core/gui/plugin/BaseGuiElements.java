/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.core.gui.plugin;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.i18n.BaseMessages;

public class BaseGuiElements {

  protected String calculateI18n( String i18nPackage, String string ) {
    if ( StringUtils.isEmpty( i18nPackage ) ) {
      return string;
    }
    if ( StringUtils.isEmpty( string ) ) {
      return null;
    }
    return BaseMessages.getString( i18nPackage, string );
  }

  protected String calculateI18nPackage( Class<?> i18nPackageClass, String i18nPackage ) {
    if ( StringUtils.isNotEmpty( i18nPackage ) ) {
      return i18nPackage;
    }
    if ( Void.class.equals( i18nPackageClass ) ) {
      return null;
    }
    return i18nPackageClass.getPackage().getName();
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
