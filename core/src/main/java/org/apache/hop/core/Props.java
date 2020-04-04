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

package org.apache.hop.core;

import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.SortedFileOutputStream;
import org.apache.hop.i18n.BaseMessages;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

/**
 * We use Props to store all kinds of user interactive information such as the selected colors, fonts, positions of
 * windows, etc.
 *
 * @author Matt
 * @since 15-12-2003
 */
public class Props implements Cloneable {
  private static Class<?> PKG = Const.class; // for i18n purposes, needed by Translator!!

  private static final String STRING_USER_PREFERENCES = "User preferences";

  protected static Props props;

  public static final String STRING_FONT_FIXED_NAME = "FontFixedName";
  public static final String STRING_FONT_FIXED_SIZE = "FontFixedSize";
  public static final String STRING_FONT_FIXED_STYLE = "FontFixedStyle";

  public static final String STRING_FONT_DEFAULT_NAME = "FontDefaultName";
  public static final String STRING_FONT_DEFAULT_SIZE = "FontDefaultSize";
  public static final String STRING_FONT_DEFAULT_STYLE = "FontDefaultStyle";

  public static final String STRING_FONT_GRAPH_NAME = "FontGraphName";
  public static final String STRING_FONT_GRAPH_SIZE = "FontGraphSize";
  public static final String STRING_FONT_GRAPH_STYLE = "FontGraphStyle";

  public static final String STRING_FONT_GRID_NAME = "FontGridName";
  public static final String STRING_FONT_GRID_SIZE = "FontGridSize";
  public static final String STRING_FONT_GRID_STYLE = "FontGridStyle";

  public static final String STRING_FONT_NOTE_NAME = "FontNoteName";
  public static final String STRING_FONT_NOTE_SIZE = "FontNoteSize";
  public static final String STRING_FONT_NOTE_STYLE = "FontNoteStyle";

  public static final String STRING_BACKGROUND_COLOR_R = "BackgroundColorR";
  public static final String STRING_BACKGROUND_COLOR_G = "BackgroundColorG";
  public static final String STRING_BACKGROUND_COLOR_B = "BackgroundColorB";

  public static final String STRING_GRAPH_COLOR_R = "GraphColorR";
  public static final String STRING_GRAPH_COLOR_G = "GraphColorG";
  public static final String STRING_GRAPH_COLOR_B = "GraphColorB";

  public static final String STRING_TAB_COLOR_R = "TabColorR54";
  public static final String STRING_TAB_COLOR_G = "TabColorG54";
  public static final String STRING_TAB_COLOR_B = "TabColorB54";

  public static final String STRING_SVG_ENABLED = "EnableSVG";
  public static final String STRING_ZOOM_FACTOR = "ZoomFactor";
  public static final String STRING_ICON_SIZE = "IconSize";
  public static final String STRING_LINE_WIDTH = "LineWidth";
  public static final String STRING_SHADOW_SIZE = "ShadowSize54";
  public static final String STRING_LOG_LEVEL = "LogLevel";
  public static final String STRING_LOG_FILTER = "LogFilter";
  public static final String STRING_MIDDLE_PCT = "MiddlePct";
  public static final String STRING_INDICATE_SLOW_PIPELINE_TRANSFORMS = "IndicateSlowPipelineTransforms";

  public static final String STRING_LAST_PREVIEW_PIPELINE = "LastPreviewPipeline";
  public static final String STRING_LAST_PREVIEW_TRANSFORM = "LastPreviewTransform";
  public static final String STRING_LAST_PREVIEW_SIZE = "LastPreviewSize";

  public static final String STRING_MAX_UNDO = "MaxUndo";

  public static final String STRING_SIZE_MAX = "SizeMax";
  public static final String STRING_SIZE_X = "SizeX";
  public static final String STRING_SIZE_Y = "SizeY";
  public static final String STRING_SIZE_W = "SizeW";
  public static final String STRING_SIZE_H = "SizeH";

  public static final String STRING_SASH_W1 = "SashWeight1";
  public static final String STRING_SASH_W2 = "SashWeight2";

  public static final String STRING_AUTO_SAVE = "AutoSave";
  public static final String STRING_SAVE_CONF = "SaveConfirmation";
  public static final String STRING_AUTO_SPLIT = "AutoSplit";
  public static final String STRING_AUTO_COLLAPSE_CORE_TREE = "AutoCollapseCoreObjectsTree";

  public static final String STRING_USE_DB_CACHE = "UseDBCache";
  public static final String STRING_OPEN_LAST_FILE = "OpenLastFile";

  public static final String STRING_ONLY_ACTIVE_TRANSFORMS = "OnlyActiveTransforms";
  public static final String STRING_START_SHOW_REPOSITORIES = "ShowRepositoriesAtStartup";
  public static final String STRING_ANTI_ALIASING = "EnableAntiAliasing54";
  public static final String STRING_SHOW_CANVAS_GRID = "ShowCanvasGrid";
  public static final String STRING_SHOW_EXIT_WARNING = "ShowExitWarning";
  public static final String STRING_SHOW_OS_LOOK = "ShowOSLook54";
  public static final String STRING_LAST_ARGUMENT = "LastArgument";

  public static final String STRING_ARGUMENT_NAME_PREFIX = "Argument ";

  public static final String STRING_CUSTOM_PARAMETER = "CustomParameter";

  public static final String STRING_PLUGIN_HISTORY = "PluginHistory";

  public static final String STRING_DEFAULT_PREVIEW_SIZE = "DefaultPreviewSize";
  public static final String STRING_ONLY_USED_DB_TO_XML = "SaveOnlyUsedConnectionsToXML";

  public static final String STRING_ASK_ABOUT_REPLACING_DATABASES = "AskAboutReplacingDatabases";
  public static final String STRING_REPLACE_DATABASES = "ReplaceDatabases";

  private static final String STRING_MAX_NR_LINES_IN_LOG = "MaxNrOfLinesInLog";
  private static final String STRING_MAX_NR_LINES_IN_HISTORY = "MaxNrOfLinesInHistory";
  private static final String STRING_LINES_IN_HISTORY_FETCH_SIZE = "LinesInHistoryFetchSize";
  public static final String STRING_DISABLE_INITIAL_EXECUTION_HISTORY = "DisableInitialExecutionHistory";
  private static final String STRING_MAX_LOG_LINE_TIMEOUT_MINUTES = "MaxLogLineTimeOutMinutes";
  public static final String STRING_RECENT_SEARCHES = "RecentSearches";

  protected ILogChannel log;
  protected Properties properties;

  protected ArrayList<ObjectUsageCount> pluginHistory;

  protected String filename;

  public static final int WIDGET_STYLE_DEFAULT = 0;
  public static final int WIDGET_STYLE_FIXED = 1;
  public static final int WIDGET_STYLE_TABLE = 2;
  public static final int WIDGET_STYLE_NOTEPAD = 3;
  public static final int WIDGET_STYLE_GRAPH = 4;
  public static final int WIDGET_STYLE_TAB = 5;
  public static final int WIDGET_STYLE_TOOLBAR = 6;

  /**
   * Initialize the properties: load from disk.
   */
  public static final void init() {
    if ( props == null ) {
      props = new Props();

    } else {
      throw new RuntimeException( "The Properties systems settings are already initialised!" );
    }
  }

  /**
   * Initialize the properties: load from disk.
   *
   * @param filename the filename to use
   */
  public static final void init( String filename ) {
    if ( props == null ) {
      props = new Props( filename );
    } else {
      throw new RuntimeException( "The properties systems settings are already initialised!" );
    }
  }

  /**
   * Check to see whether the Hop properties where loaded.
   *
   * @return true if the Hop properties where loaded.
   */
  public static boolean isInitialized() {
    return props != null;
  }

  public static Props getInstance() {
    if ( props != null ) {
      return props;
    }

    throw new RuntimeException( "Properties, Hop systems settings, not initialised!" );
  }

  protected Props() {
    initialize();
  }

  protected void initialize() {
    filename = getFilename();
    createLogChannel();
    properties = new Properties();
    pluginHistory = new ArrayList<>();

    loadProps();
    addDefaultEntries();

    loadPluginHistory();

  }

  protected Props( String filename ) {
    properties = new Properties();
    this.filename = filename;
    init();
  }

  @Override
  public String toString() {
    return STRING_USER_PREFERENCES;
  }

  protected void createLogChannel() {
    log = new LogChannel( STRING_USER_PREFERENCES );
  }

  public String getFilename() {
    String directory = Const.getHopDirectory();
    return directory + Const.FILE_SEPARATOR + ".hoprc";
  }

  public boolean fileExists() {
    File f = new File( filename );
    return f.exists();
  }

  public boolean loadProps() {
    try ( FileInputStream fis = new FileInputStream( filename ) ) {
      properties.load( fis );
    } catch ( Exception e ) {
      return false;
    }
    return true;
  }

  protected void addDefaultEntries() {
    if ( !properties.containsKey( "JobDialogStyle" ) ) {
      properties.setProperty( "JobDialogStyle", "RESIZE,MAX,MIN" );
    }
  }

  public void saveProps() {

    File spoonRc = new File( filename );
    try {
      // FileOutputStream fos = new FileOutputStream(spoonRc);

      SortedFileOutputStream fos = new SortedFileOutputStream( spoonRc );
      fos.setLogger( log );
      properties.store( fos, "Hop Properties file" );
      fos.close();
      log.logDetailed( BaseMessages.getString( PKG, "HopGui.Log.SaveProperties" ) );
    } catch ( IOException e ) {
      // If saving fails this could be a known Java bug: If running HopGui on windows the spoon
      // config file gets created with the 'hidden' attribute set. Some Java JREs cannot open
      // FileOutputStreams on files with that attribute set. The user has to unset that attribute
      // manually.
      //
      // Note that we don't really want to throw an exception here, that would prevent usage of Hop on read-only
      // systems.
      //
      if ( spoonRc.isHidden() && filename.indexOf( '\\' ) != -1 ) {
        // If filename contains a backslash we consider HopGui as running on Windows
        log.logError( BaseMessages.getString( PKG, "HopGui.Log.SavePropertiesFailedWindowsBugAttr", filename ) );
      } else {
        // Another reason why the save failed
        log.logError( BaseMessages.getString( PKG, "HopGui.Log.SavePropertiesFailed" ) + e.getMessage() );
      }
    }
  }

  public void setLogLevel( String level ) {
    properties.setProperty( STRING_LOG_LEVEL, level );
  }

  public String getLogLevel() {
    return properties.getProperty( STRING_LOG_LEVEL, "Basic" );
  }

  public void setLogFilter( String filter ) {
    properties.setProperty( STRING_LOG_FILTER, Const.NVL( filter, "" ) );
  }

  public String getLogFilter() {
    return properties.getProperty( STRING_LOG_FILTER, "" );
  }

  public void setUseDBCache( boolean use ) {
    properties.setProperty( STRING_USE_DB_CACHE, use ? "Y" : "N" );
  }

  public boolean useDBCache() {
    String use = properties.getProperty( STRING_USE_DB_CACHE );
    return !"N".equalsIgnoreCase( use );
  }

  public void setOnlyActiveTransforms( boolean only ) {
    properties.setProperty( STRING_ONLY_ACTIVE_TRANSFORMS, only ? "Y" : "N" );
  }

  public boolean getOnlyActiveTransforms() {
    String only = properties.getProperty( STRING_ONLY_ACTIVE_TRANSFORMS, "N" );
    return "Y".equalsIgnoreCase( only ); // Default: show active transforms.
  }

  public boolean askAboutReplacingDatabaseConnections() {
    String ask = properties.getProperty( STRING_ASK_ABOUT_REPLACING_DATABASES, "N" );
    return "Y".equalsIgnoreCase( ask );
  }

  public void setProperty( String propertyName, String value ) {
    properties.setProperty( propertyName, value );
  }

  public String getProperty( String propertyName ) {
    return properties.getProperty( propertyName );
  }

  public void setAskAboutReplacingDatabaseConnections( boolean ask ) {
    properties.setProperty( STRING_ASK_ABOUT_REPLACING_DATABASES, ask ? "Y" : "N" );
  }

  /**
   * @param parameterName The parameter name
   * @param defaultValue  The default value in case the parameter doesn't exist yet.
   * @return The custom parameter
   */
  public String getCustomParameter( String parameterName, String defaultValue ) {
    return properties.getProperty( STRING_CUSTOM_PARAMETER + parameterName, defaultValue );
  }

  /**
   * Set the custom parameter
   *
   * @param parameterName The name of the parameter
   * @param value         The value to be stored in the properties file.
   */
  public void setCustomParameter( String parameterName, String value ) {
    properties.setProperty( STRING_CUSTOM_PARAMETER + parameterName, value );
  }

  public void clearCustomParameters() {
    Enumeration<Object> keys = properties.keys();
    while ( keys.hasMoreElements() ) {
      String key = (String) keys.nextElement();
      if ( key.startsWith( STRING_CUSTOM_PARAMETER ) ) {
        // Clear this one
        properties.remove( key );
      }
    }
  }

  /**
   * Convert "argument 1" to 1
   *
   * @param value The value to determine the argument number for
   * @return The argument number
   */
  public static final int getArgumentNumber( IValueMeta value ) {
    if ( value != null && value.getName().startsWith( Props.STRING_ARGUMENT_NAME_PREFIX ) ) {
      return Const.toInt( value.getName().substring( Props.STRING_ARGUMENT_NAME_PREFIX.length() ), -1 );
    }
    return -1;
  }

  public static final String[] convertArguments( RowMetaAndData row ) {
    String[] args = new String[ 10 ];
    for ( int i = 0; i < row.size(); i++ ) {
      IValueMeta valueMeta = row.getValueMeta( i );
      int argNr = getArgumentNumber( valueMeta );
      if ( argNr >= 0 && argNr < 10 ) {
        try {
          args[ argNr ] = row.getString( i, "" );
        } catch ( HopValueException e ) {
          args[ argNr ] = ""; // Should never happen
        }
      }
    }
    return args;
  }

  /**
   * Set the last arguments so that we can recall it the next time...
   *
   * @param args the arguments to save
   */
  public void setLastArguments( String[] args ) {
    for ( int i = 0; i < args.length; i++ ) {
      if ( args[ i ] != null ) {
        properties.setProperty( STRING_LAST_ARGUMENT + "_" + i, args[ i ] );
      }
    }
  }

  /**
   * Get the last entered arguments...
   *
   * @return the last entered arguments...
   */
  public String[] getLastArguments() {
    String[] args = new String[ 10 ];
    for ( int i = 0; i < args.length; i++ ) {
      args[ i ] = properties.getProperty( STRING_LAST_ARGUMENT + "_" + i );
    }
    return args;
  }

  /**
   * Get the list of recently used transform
   *
   * @return a list of strings: the plug-in IDs
   */
  public List<ObjectUsageCount> getPluginHistory() {
    return pluginHistory;
  }

  public int increasePluginHistory( String pluginID ) {
    for ( int i = 0; i < pluginHistory.size(); i++ ) {
      ObjectUsageCount usage = pluginHistory.get( i );
      if ( usage.getObjectName().equalsIgnoreCase( pluginID ) ) {
        int uses = usage.increment();
        Collections.sort( pluginHistory );
        savePluginHistory();
        return uses;
      }
    }
    addPluginHistory( pluginID, 1 );
    Collections.sort( pluginHistory );
    savePluginHistory();

    return 1;
  }

  /*
   * /** Set the last plugin used in the plugin history
   *
   * @param pluginID The last plugin ID
   */
  public void addPluginHistory( String pluginID, int uses ) {
    // Add at the front
    pluginHistory.add( new ObjectUsageCount( pluginID, uses ) );
  }

  /**
   * Load the plugin history from the properties file
   */
  protected void loadPluginHistory() {
    pluginHistory = new ArrayList<>();
    int i = 0;
    String string = properties.getProperty( STRING_PLUGIN_HISTORY + "_" + i );
    while ( string != null ) {
      pluginHistory.add( ObjectUsageCount.fromString( string ) );
      i++;
      string = properties.getProperty( STRING_PLUGIN_HISTORY + "_" + i );
    }

    Collections.sort( pluginHistory );
  }

  private void savePluginHistory() {
    for ( int i = 0; i < pluginHistory.size(); i++ ) {
      ObjectUsageCount usage = pluginHistory.get( i );
      properties.setProperty( STRING_PLUGIN_HISTORY + "_" + i, usage.toString() );
    }
  }

  public boolean areOnlyUsedConnectionsSavedToXML() {
    String show = properties.getProperty( STRING_ONLY_USED_DB_TO_XML, "Y" );
    return "Y".equalsIgnoreCase( show ); // Default: save all connections
  }

  public void setOnlyUsedConnectionsSavedToXML( boolean onlyUsedConnections ) {
    properties.setProperty( STRING_ONLY_USED_DB_TO_XML, onlyUsedConnections ? "Y" : "N" );
  }

  public boolean replaceExistingDatabaseConnections() {
    String replace = properties.getProperty( STRING_REPLACE_DATABASES, "Y" );
    return "Y".equalsIgnoreCase( replace );
  }

  public void setReplaceDatabaseConnections( boolean replace ) {
    properties.setProperty( STRING_REPLACE_DATABASES, replace ? "Y" : "N" );
  }

  public int getMaxNrLinesInLog() {
    String lines = properties.getProperty( STRING_MAX_NR_LINES_IN_LOG );
    return Const.toInt( lines, Const.MAX_NR_LOG_LINES );
  }

  public void setMaxNrLinesInLog( int maxNrLinesInLog ) {
    properties.setProperty( STRING_MAX_NR_LINES_IN_LOG, Integer.toString( maxNrLinesInLog ) );
  }

  public int getMaxNrLinesInHistory() {
    String lines = properties.getProperty( STRING_MAX_NR_LINES_IN_HISTORY );
    return Const.toInt( lines, Const.MAX_NR_HISTORY_LINES );
  }

  public int getLinesInHistoryFetchSize() {
    String fetchSize = properties.getProperty( STRING_LINES_IN_HISTORY_FETCH_SIZE );
    return Const.toInt( fetchSize, Const.HISTORY_LINES_FETCH_SIZE );
  }

  public boolean disableInitialExecutionHistory() {
    String disable = properties.getProperty( STRING_DISABLE_INITIAL_EXECUTION_HISTORY, "N" );
    return "Y".equalsIgnoreCase( disable );
  }

  public void setMaxNrLinesInHistory( int maxNrLinesInHistory ) {
    properties.setProperty( STRING_MAX_NR_LINES_IN_HISTORY, Integer.toString( maxNrLinesInHistory ) );
  }

  public void setLinesInHistoryFetchSize( int linesInHistoryFetchSize ) {
    properties.setProperty( STRING_LINES_IN_HISTORY_FETCH_SIZE, Integer.toString( linesInHistoryFetchSize ) );
  }

  public void setDisableInitialExecutionHistory( boolean disable ) {
    properties.setProperty( STRING_DISABLE_INITIAL_EXECUTION_HISTORY, disable ? "Y" : "N" );
  }

  public int getMaxLogLineTimeoutMinutes() {
    String minutes = properties.getProperty( STRING_MAX_LOG_LINE_TIMEOUT_MINUTES );
    return Const.toInt( minutes, Const.MAX_LOG_LINE_TIMEOUT_MINUTES );
  }

  public void setMaxLogLineTimeoutMinutes( int maxLogLineTimeoutMinutes ) {
    properties.setProperty( STRING_MAX_LOG_LINE_TIMEOUT_MINUTES, Integer.toString( maxLogLineTimeoutMinutes ) );
  }

  public void reset() {
    props = null;
    properties.clear();
    pluginHistory.clear();
  }

}
