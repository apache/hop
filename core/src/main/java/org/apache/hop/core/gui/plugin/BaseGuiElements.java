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
