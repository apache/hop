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

package org.apache.hop.core;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * We use Props to store all kinds of user interactive information such as the selected colors, fonts, positions of
 * windows, etc.
 *
 * @author Matt
 * @since 15-12-2003
 */
public class Props implements Cloneable {
  private static final Class<?> PKG = Const.class; // For Translator

  private static final String STRING_USER_PREFERENCES = "User preferences";

  public static final String STRING_FONT_FIXED_NAME = "FontFixedName";
  public static final String STRING_FONT_FIXED_SIZE = "FontFixedSize";
  public static final String STRING_FONT_FIXED_STYLE = "FontFixedStyle";

  public static final String STRING_FONT_DEFAULT_NAME = "FontDefaultName";
  public static final String STRING_FONT_DEFAULT_SIZE = "FontDefaultSize";
  public static final String STRING_FONT_DEFAULT_STYLE = "FontDefaultStyle";

  public static final String STRING_FONT_GRAPH_NAME = "FontGraphName";
  public static final String STRING_FONT_GRAPH_SIZE = "FontGraphSize";
  public static final String STRING_FONT_GRAPH_STYLE = "FontGraphStyle";

  public static final String STRING_FONT_NOTE_NAME = "FontNoteName";
  public static final String STRING_FONT_NOTE_SIZE = "FontNoteSize";
  public static final String STRING_FONT_NOTE_STYLE = "FontNoteStyle";

  public static final String STRING_BACKGROUND_COLOR_R = "BackgroundColorR";
  public static final String STRING_BACKGROUND_COLOR_G = "BackgroundColorG";
  public static final String STRING_BACKGROUND_COLOR_B = "BackgroundColorB";

  public static final String STRING_GRAPH_COLOR_R = "GraphColorR";
  public static final String STRING_GRAPH_COLOR_G = "GraphColorG";
  public static final String STRING_GRAPH_COLOR_B = "GraphColorB";

  public static final String STRING_TAB_COLOR_R = "TabColorR";
  public static final String STRING_TAB_COLOR_G = "TabColorG";
  public static final String STRING_TAB_COLOR_B = "TabColorB";

  public static final String STRING_ZOOM_FACTOR = "ZoomFactor";
  public static final String STRING_ICON_SIZE = "IconSize";
  public static final String STRING_LINE_WIDTH = "LineWidth";
  public static final String STRING_MIDDLE_PCT = "MiddlePct";
  public static final String STRING_INDICATE_SLOW_PIPELINE_TRANSFORMS = "IndicateSlowPipelineTransforms";

  public static final String STRING_LAST_PREVIEW_PIPELINE = "LastPreviewPipeline";
  public static final String STRING_LAST_PREVIEW_TRANSFORM = "LastPreviewTransform";
  public static final String STRING_LAST_PREVIEW_SIZE = "LastPreviewSize";

  public static final String STRING_MAX_UNDO = "MaxUndo";

  public static final String STRING_AUTO_SAVE = "AutoSave";
  public static final String STRING_SAVE_CONF = "SaveConfirmation";
  public static final String STRING_AUTO_SPLIT = "AutoSplit";
  public static final String STRING_AUTO_COLLAPSE_CORE_TREE = "AutoCollapseCoreObjectsTree";

  public static final String STRING_USE_DB_CACHE = "UseDBCache";
  public static final String STRING_OPEN_LAST_FILE = "OpenLastFile";

  public static final String STRING_SHOW_CANVAS_GRID = "ShowCanvasGrid";
  public static final String STRING_SHOW_EXIT_WARNING = "ShowExitWarning";
  public static final String STRING_SHOW_OS_LOOK = "ShowOSLook";

  public static final String STRING_CUSTOM_PARAMETER = "CustomParameter";

  public static final String STRING_DEFAULT_PREVIEW_SIZE = "DefaultPreviewSize";

  protected ILogChannel log;

  public static final int WIDGET_STYLE_DEFAULT = 0;
  public static final int WIDGET_STYLE_FIXED = 1;
  public static final int WIDGET_STYLE_TABLE = 2;
  public static final int WIDGET_STYLE_NOTEPAD = 3;
  public static final int WIDGET_STYLE_GRAPH = 4;
  public static final int WIDGET_STYLE_TAB = 5;
  public static final int WIDGET_STYLE_TOOLBAR = 6;

  public Props() {
    log = new LogChannel( STRING_USER_PREFERENCES );
  }

  @Override
  public String toString() {
    return STRING_USER_PREFERENCES;
  }


  protected void setDefault() {
    if ( !containsKey( "WorkflowDialogStyle" ) ) {
      setProperty( "WorkflowDialogStyle", "RESIZE,MAX,MIN" );
    }
  }

  protected void setProperty(String key, String value) {
    try {
      HopConfig.setGuiProperty( key, value );
      HopConfig.getInstance().saveToFile();
    } catch(Exception e) {
      throw new RuntimeException("Error saving hop config option key '"+key+"', value '"+value+"'", e);
    }
  }

  public String getProperty( String propertyName ) {
    return getProperty( propertyName, null );
  }

  public String getProperty( String propertyName, String defaultValue ) {
    String value = HopConfig.getGuiProperty( propertyName );
    if ( StringUtils.isEmpty(value)) {
      return defaultValue;
    }
    return value;
  }

  public boolean containsKey(String key) {
    return HopConfig.getInstance().getConfigMap().containsKey( key );
  }

  public void setUseDBCache( boolean use ) {
    setProperty( STRING_USE_DB_CACHE, use ? "Y" : "N" );
  }

  public boolean useDBCache() {
    String use = getProperty( STRING_USE_DB_CACHE );
    return !"N".equalsIgnoreCase( use );
  }



  /**
   * @param parameterName The parameter name
   * @param defaultValue  The default value in case the parameter doesn't exist yet.
   * @return The custom parameter
   */
  public String getCustomParameter( String parameterName, String defaultValue ) {
    return getProperty( STRING_CUSTOM_PARAMETER + parameterName, defaultValue );
  }

  /**
   * Set the custom parameter
   *
   * @param parameterName The name of the parameter
   * @param value         The value to be stored in the properties file.
   */
  public void setCustomParameter( String parameterName, String value ) {
    setProperty( STRING_CUSTOM_PARAMETER + parameterName, value );
  }

  public void clearCustomParameters() throws HopException {
    Map<String, String> configMap = HopConfig.readGuiProperties();
    Iterator<Map.Entry<String, String>> iterator = configMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      if (entry.getKey().startsWith( STRING_CUSTOM_PARAMETER )) {
        iterator.remove();
      }
    }
    HopConfig.getInstance().saveToFile();
  }

  public void reset() {
    clear();
  }

  private void clear() {
    HopConfig.getInstance().getConfigMap().clear();
  }


}
