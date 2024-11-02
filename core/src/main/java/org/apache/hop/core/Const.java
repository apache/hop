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

package org.apache.hop.core;

import java.awt.Font;
import java.awt.GraphicsEnvironment;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang.text.StrBuilder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.Variable;
import org.apache.hop.core.variables.VariableScope;
import org.apache.hop.i18n.BaseMessages;

/**
 * This class is used to define a number of default values for various settings throughout Hop. It
 * also contains a number of static final methods to make your life easier.
 */
public class Const {
  private static final Class<?> PKG = Const.class;

  /** Release Type */
  public enum ReleaseType {
    RELEASE_CANDIDATE {
      @Override
      public String getMessage() {
        return BaseMessages.getString(PKG, "Const.PreviewRelease.HelpAboutText");
      }
    },
    MILESTONE {
      @Override
      public String getMessage() {
        return BaseMessages.getString(PKG, "Const.Candidate.HelpAboutText");
      }
    },
    PREVIEW {
      @Override
      public String getMessage() {
        return BaseMessages.getString(PKG, "Const.Milestone.HelpAboutText");
      }
    },
    GA {
      @Override
      public String getMessage() {
        return BaseMessages.getString(PKG, "Const.GA.HelpAboutText");
      }
    };

    public abstract String getMessage();
  }

  private static final String CONST_WINDOWS = "Windows";
  private static final String CONST_LINUX = "Linux";

  /** Sleep time waiting when buffer is empty (the default) */
  public static final int TIMEOUT_GET_MILLIS = 50;

  /** Sleep time waiting when buffer is full (the default) */
  public static final int TIMEOUT_PUT_MILLIS = 50;

  /** print update every ... lines */
  public static final int ROWS_UPDATE = 50000;

  /** Size of rowset: bigger = faster for large amounts of data */
  public static final int ROWS_IN_ROWSET = 10000;

  /** Fetch size in rows when querying a database */
  public static final int FETCH_SIZE = 10000;

  /** What's the file systems file separator on this operating system? */
  public static final String FILE_SEPARATOR = System.getProperty("file.separator");

  /** What's the path separator on this operating system? */
  public static final String PATH_SEPARATOR = System.getProperty("path.separator");

  /** CR: operating systems specific Carriage Return */
  public static final String CR = System.getProperty("line.separator");

  /*
   The name of the history folder in which the Hop local audit manager saves data
  */
  @Variable(
      scope = VariableScope.SYSTEM,
      description =
          "The name of the history folder in which the Hop local audit manager saves data.")
  public static final String HOP_AUDIT_FOLDER =
      NVL(
          System.getProperty("HOP_AUDIT_FOLDER"),
          System.getProperty("user.dir") + File.separator + "audit");

  /*
   The name of the history folder in which the Hop local audit manager saves data
  */
  @Variable(
      scope = VariableScope.SYSTEM,
      description =
          "The name of the history folder in which the Hop local audit manager saves data.")
  public static final String HOP_CONFIG_FOLDER =
      NVL(
          System.getProperty("HOP_CONFIG_FOLDER"),
          System.getProperty("user.dir") + File.separator + "config");

  /**
   * The name of the variable which configures whether or not we should automatically create a
   * config file when it's missing
   */
  @Variable(
      scope = VariableScope.SYSTEM,
      value = "N",
      description =
          "Set this variable to 'Y' to automatically create config file when it's missing.")
  public static final String HOP_AUTO_CREATE_CONFIG = "HOP_AUTO_CREATE_CONFIG";

  /**
   * The system environment variable pointing to the alternative location for the Hop metadata
   * folder
   */
  @Variable(
      scope = VariableScope.SYSTEM,
      description = "The variable which points to the alternative location for the Hop metadata.")
  public static final String HOP_METADATA_FOLDER = "HOP_METADATA_FOLDER";

  /** A comma separated list pointing to folders with JDBC drivers to add. */
  @Variable(
      scope = VariableScope.SYSTEM,
      description = "A comma separated list pointing to folders with JDBC drivers to add.")
  public static final String HOP_SHARED_JDBC_FOLDERS = "HOP_SHARED_JDBC_FOLDERS";

  /** The operating system the hop platform runs on */
  @Variable(
      scope = VariableScope.SYSTEM,
      description = "The operating system the hop platform runs on.")
  public static final String HOP_PLATFORM_OS = "HOP_PLATFORM_OS";

  /** The runtime that is being used */
  @Variable(scope = VariableScope.SYSTEM, description = "The runtime that is being used.")
  public static final String HOP_PLATFORM_RUNTIME = "HOP_PLATFORM_RUNTIME";

  /** An empty ("") String. */
  public static final String EMPTY_STRING = "";

  /** the default comma separated list of base plugin folders. */
  public static final String DEFAULT_PLUGIN_BASE_FOLDERS = "plugins";

  /** Default minimum date range... */
  public static final Date MIN_DATE = new Date(-2208992400000L); // 1900/01/01 00:00:00.000

  /** Default maximum date range... */
  public static final Date MAX_DATE = new Date(7258114799468L); // 2199/12/31 23:59:59.999

  /** The default minimum year in a dimension date range */
  public static final int MIN_YEAR = 1900;

  /** The default maximum year in a dimension date range */
  public static final int MAX_YEAR = 2199;

  /** Specifies the number of pixels to the right we have to go in dialog boxes. */
  public static final int RIGHT = 400;

  /** Specifies the length (width) of fields in a number of pixels in dialog boxes. */
  public static final int LENGTH = 350;

  /**
   * @deprecated The margin between the different dialog components &amp; widgets. This method is
   *     deprecated. Please use PropsUi.getMargin() instead.
   */
  @Deprecated(since = "2.10")
  public static final int MARGIN = 4;

  /**
   * @deprecated The default percentage of the width of screen where we consider the middle of a
   *     dialog. This method is deprecated. Please use PropsUi.getMiddlePct() instead.
   */
  @Deprecated(since = "2.10")
  public static final int MIDDLE_PCT = 35;

  /**
   * @deprecated The horizontal and vertical margin of a dialog box. This method is deprecated.
   *     Please use PropsUi.getFormMargin() instead.
   */
  @Deprecated(since = "2.10")
  public static final int FORM_MARGIN = 5;

  /** The default locale for the hop environment (system defined) */
  public static final Locale DEFAULT_LOCALE = Locale.getDefault();

  /** The default decimal separator . or , */
  public static final char DEFAULT_DECIMAL_SEPARATOR =
      (new DecimalFormatSymbols(DEFAULT_LOCALE)).getDecimalSeparator();

  /** The default grouping separator , or . */
  public static final char DEFAULT_GROUPING_SEPARATOR =
      (new DecimalFormatSymbols(DEFAULT_LOCALE)).getGroupingSeparator();

  /** The default currency symbol */
  public static final String DEFAULT_CURRENCY_SYMBOL =
      (new DecimalFormatSymbols(DEFAULT_LOCALE)).getCurrencySymbol();

  /** The default number format */
  public static final String DEFAULT_NUMBER_FORMAT =
      ((DecimalFormat) (NumberFormat.getInstance())).toPattern();

  /** Default string representing Null String values (empty) */
  public static final String NULL_STRING = "";

  /** Default string representing Null Number values (empty) */
  public static final String NULL_NUMBER = "";

  /** Default string representing Null Date values (empty) */
  public static final String NULL_DATE = "";

  /** Default string representing Null BigNumber values (empty) */
  public static final String NULL_BIGNUMBER = "";

  /** Default string representing Null Boolean values (empty) */
  public static final String NULL_BOOLEAN = "";

  /** Default string representing Null Integer values (empty) */
  public static final String NULL_INTEGER = "";

  /** Default string representing Null Binary values (empty) */
  public static final String NULL_BINARY = "";

  /** Default string representing Null Undefined values (empty) */
  public static final String NULL_NONE = "";

  /**
   * Rounding mode, not implemented in {@code BigDecimal}. Method java.lang.Math.round(double)
   * processes this way. <br>
   * Rounding mode to round towards {@literal "nearest neighbor"} unless both neighbors are
   * equidistant, in which case round ceiling. <br>
   * Behaves as for {@code ROUND_CEILING} if the discarded fraction is &ge; 0.5; otherwise, behaves
   * as for {@code ROUND_FLOOR}. Note that this is the most common arithmetical rounding mode.
   */
  public static final int ROUND_HALF_CEILING = -1;

  /** An array of date conversion formats */
  private static String[] dateFormats;

  /** An array of number conversion formats */
  private static String[] numberFormats;

  /**
   * Generalized date/time format: Wherever dates are used, date and time values are organized from
   * the most to the least significant. see also method StringUtil.getFormattedDateTime()
   */
  public static final String GENERALIZED_DATE_TIME_FORMAT = "yyyyddMM_hhmmss";

  public static final String GENERALIZED_DATE_TIME_FORMAT_MILLIS = "yyyyddMM_hhmmssSSS";

  /** Default we store our information in Unicode UTF-8 character set. */
  public static final String XML_ENCODING = "UTF-8";

  /** Allow or disallow doctype declarations in XML. " */
  @Variable(value = "N", description = "A variable allow or disallow doctype declarations in XML")
  public static final String XML_ALLOW_DOCTYPE_DECL = "XML_ALLOW_DOCTYPE_DECL";

  /** Name of the hop configuration file */
  public static final String HOP_CONFIG = "hop-config.json";

  /** The prefix that all internal hop variables should have */
  public static final String INTERNAL_VARIABLE_PREFIX = "Internal";

  /** The workflow filename directory */
  public static final String INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER =
      INTERNAL_VARIABLE_PREFIX + ".Workflow.Filename.Folder";

  /** The workflow filename name */
  public static final String INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME =
      INTERNAL_VARIABLE_PREFIX + ".Workflow.Filename.Name";

  /** The workflow name */
  public static final String INTERNAL_VARIABLE_WORKFLOW_NAME =
      INTERNAL_VARIABLE_PREFIX + ".Workflow.Name";

  /** The workflow ID */
  public static final String INTERNAL_VARIABLE_WORKFLOW_ID =
      INTERNAL_VARIABLE_PREFIX + ".Workflow.ID";

  /** The workflow parent ID */
  public static final String INTERNAL_VARIABLE_WORKFLOW_PARENT_ID =
      INTERNAL_VARIABLE_PREFIX + ".Workflow.ParentID";

  /** The current pipeline directory */
  public static final String INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER =
      INTERNAL_VARIABLE_PREFIX + ".Entry.Current.Folder";

  /** All the internal pipeline variables */
  public static final Set<String> INTERNAL_PIPELINE_VARIABLES =
      Set.of(
          Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER,
          Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY,
          Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_NAME,
          Const.INTERNAL_VARIABLE_PIPELINE_NAME);

  /** All the internal workflow variables */
  public static final Set<String> INTERNAL_WORKFLOW_VARIABLES =
      Set.of(
          Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER,
          Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER,
          Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME,
          Const.INTERNAL_VARIABLE_WORKFLOW_NAME);

  /** The pipeline filename directory */
  public static final String INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY =
      INTERNAL_VARIABLE_PREFIX + ".Pipeline.Filename.Directory";

  /** The pipeline filename name */
  public static final String INTERNAL_VARIABLE_PIPELINE_FILENAME_NAME =
      INTERNAL_VARIABLE_PREFIX + ".Pipeline.Filename.Name";

  /** The pipeline name */
  public static final String INTERNAL_VARIABLE_PIPELINE_NAME =
      INTERNAL_VARIABLE_PREFIX + ".Pipeline.Name";

  /** The pipeline ID */
  public static final String INTERNAL_VARIABLE_PIPELINE_ID =
      INTERNAL_VARIABLE_PREFIX + ".Pipeline.ID";

  /** The pipeline parent ID */
  public static final String INTERNAL_VARIABLE_PIPELINE_PARENT_ID =
      INTERNAL_VARIABLE_PREFIX + ".Pipeline.ParentID";

  /** The transform partition ID */
  public static final String INTERNAL_VARIABLE_TRANSFORM_PARTITION_ID =
      INTERNAL_VARIABLE_PREFIX + ".Transform.Partition.ID";

  /** The transform partition number */
  public static final String INTERNAL_VARIABLE_TRANSFORM_PARTITION_NR =
      INTERNAL_VARIABLE_PREFIX + ".Transform.Partition.Number";

  /** The transform name */
  public static final String INTERNAL_VARIABLE_TRANSFORM_NAME =
      INTERNAL_VARIABLE_PREFIX + ".Transform.Name";

  /** The transform copy nr */
  public static final String INTERNAL_VARIABLE_TRANSFORM_COPYNR =
      INTERNAL_VARIABLE_PREFIX + ".Transform.CopyNr";

  public static final String INTERNAL_VARIABLE_TRANSFORM_ID =
      INTERNAL_VARIABLE_PREFIX + ".Transform.ID";

  public static final String INTERNAL_VARIABLE_TRANSFORM_BUNDLE_NR =
      INTERNAL_VARIABLE_PREFIX + ".Transform.BundleNr";

  public static final String INTERNAL_VARIABLE_ACTION_ID = INTERNAL_VARIABLE_PREFIX + ".Action.ID";

  /** The default maximum for the nr of lines in the GUI logs */
  public static final int MAX_NR_LOG_LINES = 5000;

  /** The default maximum for the nr of lines in the history views */
  public static final int MAX_NR_HISTORY_LINES = 50;

  /** The default fetch size for lines of history. */
  public static final int HISTORY_LINES_FETCH_SIZE = 10;

  /** The default log line timeout in minutes : 12 hours */
  public static final int MAX_LOG_LINE_TIMEOUT_MINUTES = 12 * 60;

  /** UI-agnostic flag for warnings */
  public static final int WARNING = 1;

  /** UI-agnostic flag for warnings */
  public static final int ERROR = 2;

  /** UI-agnostic flag for warnings */
  public static final int INFO = 3;

  public static final int SHOW_MESSAGE_DIALOG_DB_TEST_DEFAULT = 0;

  public static final int SHOW_MESSAGE_DIALOG_DB_TEST_SUCCESS = 1;

  /** The margin between the text of a note and its border. */
  public static final int NOTE_MARGIN = 5;

  /** The default undo level for Hop */
  public static final int MAX_UNDO = 100;

  /**
   * If you set this environment variable you can limit the log size of all pipelines and workflows
   * that don't have the "log size limit" property set in their respective properties.
   */
  @Variable(
      value = "0",
      description =
          "The log size limit for all pipelines and workflows that don't have the \"log size limit\" property set in their respective properties.")
  public static final String HOP_LOG_SIZE_LIMIT = "HOP_LOG_SIZE_LIMIT";

  /** The name of the variable that defines the timer used for detecting server nodes. */
  @Variable(
      description =
          "The name of the variable that defines the timer used for detecting server nodes")
  public static final String HOP_SERVER_DETECTION_TIMER = "HOP_SERVER_DETECTION_TIMER";

  /**
   * System wide flag to drive the evaluation of null in ValueMeta. If this setting is set to "Y",
   * an empty string and null are different. Otherwise they are not.
   */
  @Variable(
      value = "N",
      description =
          "NULL vs Empty String. If this setting is set to 'Y', an empty string and null are different. Otherwise they are not")
  public static final String HOP_EMPTY_STRING_DIFFERS_FROM_NULL =
      "HOP_EMPTY_STRING_DIFFERS_FROM_NULL";

  /**
   * System wide flag to allow non-strict string to number conversion for backward compatibility. If
   * this setting is set to "Y", an string starting with digits will be converted successfully into
   * a number. (example: 192.168.1.1 will be converted into 192 or 192.168 depending on the decimal
   * symbol). The default (N) will be to throw an error if non-numeric symbols are found in the
   * string.
   */
  @Variable(
      value = "N",
      description =
          "System wide flag to allow lenient string to number conversion for backward compatibility. If this setting is set to 'Y', an string starting with digits will be converted successfully into a number. (example: 192.168.1.1 will be converted into 192 or 192.168 or 192168 depending on the decimal and grouping symbol). The default (N) will be to throw an error if non-numeric symbols are found in the string.")
  public static final String HOP_LENIENT_STRING_TO_NUMBER_CONVERSION =
      "HOP_LENIENT_STRING_TO_NUMBER_CONVERSION";

  /**
   * You can use this variable to speed up hostname lookup. Hostname lookup is performed by Hop so
   * that it is capable of logging the server on which a workflow or pipeline is executed.
   */
  @Variable(
      description =
          "You can use this variable to speed up hostname lookup. Hostname lookup is performed by Hop so that it is capable of logging the server on which a workflow or pipeline is executed.")
  public static final String HOP_SYSTEM_HOSTNAME = "HOP_SYSTEM_HOSTNAME";

  /**
   * System wide flag to set the maximum number of log lines that are kept internally by Hop. Set to
   * 0 to keep all rows (default)
   */
  @Variable(
      scope = VariableScope.APPLICATION,
      value = "0",
      description =
          "The maximum number of log lines that are kept internally by Hop. Set to 0 to keep all rows (default)")
  public static final String HOP_MAX_LOG_SIZE_IN_LINES = "HOP_MAX_LOG_SIZE_IN_LINES";

  /**
   * System wide flag to set the maximum age (in minutes) of a log line while being kept internally
   * by Hop. Set to 0 to keep all rows indefinitely (default)
   */
  @Variable(
      scope = VariableScope.APPLICATION,
      value = "1440",
      description =
          "The maximum age (in minutes) of a log line while being kept internally by Hop. Set to 0 to keep all rows indefinitely (default)")
  public static final String HOP_MAX_LOG_TIMEOUT_IN_MINUTES = "HOP_MAX_LOG_TIMEOUT_IN_MINUTES";

  /**
   * System wide flag to determine whether standard error will be redirected to Hop logging
   * facilities. Will redirect if the value is equal ignoring case to the string "Y"
   */
  @Variable(
      scope = VariableScope.SYSTEM,
      value = "N",
      description = "Set this variable to 'Y' to redirect stderr to Hop logging.")
  public static final String HOP_REDIRECT_STDERR = "HOP_REDIRECT_STDERR";

  /**
   * System wide flag to determine whether standard out will be redirected to Hop logging
   * facilities. Will redirect if the value is equal ignoring case to the string "Y"
   */
  @Variable(
      scope = VariableScope.SYSTEM,
      value = "N",
      description = "Set this variable to 'Y' to redirect stdout to Hop logging.")
  public static final String HOP_REDIRECT_STDOUT = "HOP_REDIRECT_STDOUT";

  /** System wide flag to log stack traces in a simpler, more human readable format */
  @Variable(
      scope = VariableScope.SYSTEM,
      value = "N",
      description =
          "Set this variable to 'Y' to log stack traces in a simpler, more human readable format.")
  public static final String HOP_SIMPLE_STACK_TRACES = "HOP_SIMPLE_STACK_TRACES";

  public static final boolean isUsingSimpleStackTraces() {
    return toBoolean(System.getProperty(Const.HOP_SIMPLE_STACK_TRACES));
  }

  public static boolean toBoolean(String string) {
    return "y".equalsIgnoreCase(string)
        || "yes".equalsIgnoreCase(string)
        || "true".equalsIgnoreCase(string)
        || "t".equalsIgnoreCase(string);
  }

  /**
   * This environment variable will set a time-out after which waiting, completed or stopped
   * pipelines and workflows will be automatically cleaned up. The default value is 1440 (one day).
   */
  @Variable(
      value = "1440",
      description =
          "This project variable will set a time-out after which waiting, completed or stopped pipelines and workflows will be automatically cleaned up. The default value is 1440 (one day).")
  public static final String HOP_SERVER_OBJECT_TIMEOUT_MINUTES =
      "HOP_SERVER_OBJECT_TIMEOUT_MINUTES";

  /**
   * System wide parameter: the maximum number of transform performance snapshots to keep in memory.
   * Set to 0 to keep all snapshots indefinitely (default)
   */
  @Variable(
      value = "0",
      description =
          "The maximum number of transform performance snapshots to keep in memory. Set to 0 to keep all snapshots indefinitely (default)")
  public static final String HOP_TRANSFORM_PERFORMANCE_SNAPSHOT_LIMIT =
      "HOP_TRANSFORM_PERFORMANCE_SNAPSHOT_LIMIT";

  /** A variable to configure the maximum number of workflow trackers kept in memory. */
  @Variable(
      value = "5000",
      description =
          "The maximum age (in minutes) of a log line while being kept internally by Hop. Set to 0 to keep all rows indefinitely (default)")
  public static final String HOP_MAX_WORKFLOW_TRACKER_SIZE = "HOP_MAX_WORKFLOW_TRACKER_SIZE";

  /**
   * A variable to configure the maximum number of action results kept in memory for logging
   * purposes.
   */
  @Variable(
      value = "5000",
      description = "The maximum number of action results kept in memory for logging purposes.")
  public static final String HOP_MAX_ACTIONS_LOGGED = "HOP_MAX_ACTIONS_LOGGED";

  /**
   * A variable to configure the maximum number of logging registry entries kept in memory for
   * logging purposes.
   */
  @Variable(
      value = "10000",
      description =
          "The maximum number of logging registry entries kept in memory for logging purposes")
  public static final String HOP_MAX_LOGGING_REGISTRY_SIZE = "HOP_MAX_LOGGING_REGISTRY_SIZE";

  /** A variable to configure the hop log tab refresh delay. */
  @Variable(
      scope = VariableScope.APPLICATION,
      value = "1000",
      description = "The hop log tab refresh delay.")
  public static final String HOP_LOG_TAB_REFRESH_DELAY = "HOP_LOG_TAB_REFRESH_DELAY";

  /** A variable to configure the hop log tab refresh period. */
  @Variable(
      scope = VariableScope.APPLICATION,
      value = "1000",
      description = "The hop log tab refresh period.")
  public static final String HOP_LOG_TAB_REFRESH_PERIOD = "HOP_LOG_TAB_REFRESH_PERIOD";

  /**
   * Name of the environment variable to specify additional classes to scan for plugin annotations
   */
  @Variable(description = "A comma delimited list of classes to scan for plugin annotations")
  public static final String HOP_PLUGIN_CLASSES = "HOP_PLUGIN_CLASSES";

  /** Name of the environment variable to specify alternative location for plugins. */
  @Variable(
      scope = VariableScope.SYSTEM,
      description = "The variable which points to the alternative location for plugins.")
  public static final String HOP_PLUGIN_BASE_FOLDERS = "HOP_PLUGIN_BASE_FOLDERS";

  /**
   * Name of the environment variable that contains the size of the pipeline rowset size. This
   * overwrites values that you set pipeline settings.
   */
  @Variable(
      description =
          "Name of the environment variable that contains the size of the pipeline rowset size. This overwrites values that you set pipeline settings")
  public static final String HOP_PIPELINE_ROWSET_SIZE = "HOP_PIPELINE_ROWSET_SIZE";

  /** A general initial version comment */
  public static final String VERSION_COMMENT_INITIAL_VERSION = "Creation of initial version";

  /** A general edit version comment */
  public static final String VERSION_COMMENT_EDIT_VERSION = "Modification by user";

  /** Specifies the password encoding plugin to use by ID (Hop is the default). */
  @Variable(
      value = "Hop",
      description = "Specifies the password encoder plugin to use by ID (Hop is the default).")
  public static final String HOP_PASSWORD_ENCODER_PLUGIN = "HOP_PASSWORD_ENCODER_PLUGIN";

  /**
   * The name of the Hop encryption seed environment variable for the HopTwoWayPasswordEncoder class
   */
  public static final String HOP_TWO_WAY_PASSWORD_ENCODER_SEED =
      "HOP_TWO_WAY_PASSWORD_ENCODER_SEED";

  /**
   * The name of the variable that optionally contains an alternative rowset get timeout (in ms).
   * This only makes a difference for extremely short lived pipelines.
   */
  @Variable(
      value = "50",
      description =
          "The name of the variable that optionally contains an alternative rowset get timeout (in ms). This only makes a difference for extremely short lived pipelines.")
  public static final String HOP_ROWSET_GET_TIMEOUT = "HOP_ROWSET_GET_TIMEOUT";

  /**
   * The name of the variable that optionally contains an alternative rowset put timeout (in ms).
   * This only makes a difference for extremely short lived pipelines.
   */
  @Variable(
      value = "50",
      description =
          "The name of the variable that optionally contains an alternative rowset put timeout (in ms). This only makes a difference for extremely short lived pipelines.")
  public static final String HOP_ROWSET_PUT_TIMEOUT = "HOP_ROWSET_PUT_TIMEOUT";

  /** Set this variable to Y if you want to test a more efficient batching row set. (default = N) */
  @Variable(
      value = "N",
      description =
          "Set this variable to 'Y' if you want to test a more efficient batching row set.")
  public static final String HOP_BATCHING_ROWSET = "HOP_BATCHING_ROWSET";

  /**
   * Set this variable to limit max number of files the Text File Output transform can have open at
   * one time.
   */
  @Variable(
      value = "1024",
      description =
          "This project variable is used by the Text File Output transform. It defines the max number of simultaneously open files within the transform. The transform will close/reopen files as necessary to insure the max is not exceeded")
  public static final String HOP_FILE_OUTPUT_MAX_STREAM_COUNT = "HOP_FILE_OUTPUT_MAX_STREAM_COUNT";

  /**
   * This variable contains the number of milliseconds between flushes of all open files in the Text
   * File Output transform.
   */
  @Variable(
      value = "0",
      description =
          "This project variable is used by the Text File Output transform. It defines the max number of milliseconds between flushes of files opened by the transform.")
  public static final String HOP_FILE_OUTPUT_MAX_STREAM_LIFE = "HOP_FILE_OUTPUT_MAX_STREAM_LIFE";

  /** Set this variable to Y to disable standard Hop logging to the console. (stdout) */
  @Variable(
      value = "N",
      description =
          "Set this variable to 'Y' to disable standard Hop logging to the console. (stdout)")
  public static final String HOP_DISABLE_CONSOLE_LOGGING = "HOP_DISABLE_CONSOLE_LOGGING";

  /** The name of the variable containing an alternative default number format */
  @Variable(
      description = "The name of the variable containing an alternative default number format")
  public static final String HOP_DEFAULT_NUMBER_FORMAT = "HOP_DEFAULT_NUMBER_FORMAT";

  /** The name of the variable containing an alternative default bignumber format */
  @Variable(
      description = "The name of the variable containing an alternative default bignumber format")
  public static final String HOP_DEFAULT_BIGNUMBER_FORMAT = "HOP_DEFAULT_BIGNUMBER_FORMAT";

  /** The name of the variable containing an alternative default integer format */
  @Variable(
      description = "The name of the variable containing an alternative default integer format")
  public static final String HOP_DEFAULT_INTEGER_FORMAT = "HOP_DEFAULT_INTEGER_FORMAT";

  /** The name of the variable containing an alternative default date format */
  @Variable(description = "The name of the variable containing an alternative default date format")
  public static final String HOP_DEFAULT_DATE_FORMAT = "HOP_DEFAULT_DATE_FORMAT";

  // Null values tweaks
  @Variable(
      value = "N",
      description =
          "Set this variable to 'Y' to set the minimum to NULL if NULL is within an aggregate. Otherwise by default NULL is ignored by the MIN aggregate and MIN is set to the minimum value that is not NULL. See also the variable HOP_AGGREGATION_ALL_NULLS_ARE_ZERO.")
  public static final String HOP_AGGREGATION_MIN_NULL_IS_VALUED =
      "HOP_AGGREGATION_MIN_NULL_IS_VALUED";

  @Variable(
      value = "N",
      description =
          "Set this variable to 'Y' to return 0 when all values within an aggregate are NULL. Otherwise by default a NULL is returned when all values are NULL.")
  public static final String HOP_AGGREGATION_ALL_NULLS_ARE_ZERO =
      "HOP_AGGREGATION_ALL_NULLS_ARE_ZERO";

  /** The name of the variable containing an alternative default timestamp format */
  @Variable(
      description = "The name of the variable containing an alternative default timestamp format")
  public static final String HOP_DEFAULT_TIMESTAMP_FORMAT = "HOP_DEFAULT_TIMESTAMP_FORMAT";

  /** Variable that is responsible for removing enclosure symbol after splitting the string */
  @Variable(
      value = "N",
      description =
          "Set this variable to 'N' to preserve enclosure symbol after splitting the string in the Split fields transform. Changing it to true will remove first and last enclosure symbol from the resulting string chunks.")
  public static final String HOP_SPLIT_FIELDS_REMOVE_ENCLOSURE =
      "HOP_SPLIT_FIELDS_REMOVE_ENCLOSURE";

  /** Variable that is responsible for checking empty field names and types. */
  @Variable(
      value = "N",
      description =
          "Set this variable to 'Y' to allow your pipeline to pass 'null' fields and/or empty types.")
  public static final String HOP_ALLOW_EMPTY_FIELD_NAMES_AND_TYPES =
      "HOP_ALLOW_EMPTY_FIELD_NAMES_AND_TYPES";

  /**
   * Set this variable to false to preserve global log variables defined in pipeline / workflow
   * Properties -> Log panel. Changing it to true will clear all global log variables when export
   * pipeline / workflow
   */
  @Variable(
      value = "N",
      description =
          "Set this variable to 'N' to preserve global log variables defined in pipeline / workflow Properties -> Log panel. Changing it to 'Y' will clear it when export pipeline / workflow.")
  public static final String HOP_GLOBAL_LOG_VARIABLES_CLEAR_ON_EXPORT =
      "HOP_GLOBAL_LOG_VARIABLES_CLEAR_ON_EXPORT";

  private static String[] emptyPaddedSpacesStrings;

  /** The release type of this compilation */
  public static final ReleaseType RELEASE = ReleaseType.GA;

  /** A variable to configure turning on/off detailed subjects in log. */
  @Variable(
      value = "N",
      description =
          "Set this variable to 'Y' to precede transform/action name in log lines with the complete path to the transform/action. Useful to perfectly identify where a problem happened in our process.")
  public static final String HOP_LOG_MARK_MAPPINGS = "HOP_LOG_MARK_MAPPINGS";

  /** A variable to configure jetty option: acceptors for Hop server */
  @Variable(description = "A variable to configure jetty option: acceptors for Hop server")
  public static final String HOP_SERVER_JETTY_ACCEPTORS = "HOP_SERVER_JETTY_ACCEPTORS";

  /** A variable to configure jetty option: acceptQueueSize for Hop server */
  @Variable(description = "A variable to configure jetty option: acceptQueueSize for Hop server")
  public static final String HOP_SERVER_JETTY_ACCEPT_QUEUE_SIZE =
      "HOP_SERVER_JETTY_ACCEPT_QUEUE_SIZE";

  /** A variable to configure jetty option: lowResourcesMaxIdleTime for Hop server */
  @Variable(
      description = "A variable to configure jetty option: lowResourcesMaxIdleTime for Hop server")
  public static final String HOP_SERVER_JETTY_RES_MAX_IDLE_TIME =
      "HOP_SERVER_JETTY_RES_MAX_IDLE_TIME";

  @Variable(
      description =
          "Defines the default encoding for servlets, leave it empty to use Java default encoding")
  public static final String HOP_DEFAULT_SERVLET_ENCODING = "HOP_DEFAULT_SERVLET_ENCODING";

  /** A variable to configure refresh for Hop server workflow/pipeline status page */
  @Variable(
      description = "A variable to configure refresh for Hop server workflow/pipeline status page")
  public static final String HOP_SERVER_REFRESH_STATUS = "HOP_SERVER_REFRESH_STATUS";

  /** A variable to configure s3vfs to use a temporary file on upload data to S3 Amazon." */
  public static final String S3VFS_USE_TEMPORARY_FILE_ON_UPLOAD_DATA =
      "s3.vfs.useTempFileOnUploadData";

  /** A variable to configure Tab size" */
  @Variable(description = "A variable to configure Tab size")
  public static final String HOP_MAX_TAB_LENGTH = "HOP_MAX_TAB_LENGTH";

  /**
   * A variable to configure VFS USER_DIR_IS_ROOT option: should be "true" or "false" {@linkplain
   * org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder}
   */
  public static final String VFS_USER_DIR_IS_ROOT = "vfs.sftp.userDirIsRoot";

  /**
   * A variable to configure the minimum allowed ratio between de- and inflated bytes to detect a
   * zipbomb.
   *
   * <p>If not set or if the configured value is invalid, it defaults to {@link
   * #HOP_ZIP_MIN_INFLATE_RATIO_DEFAULT}
   *
   * @see #HOP_ZIP_MIN_INFLATE_RATIO_DEFAULT
   * @see #HOP_ZIP_MIN_INFLATE_RATIO_DEFAULT_STRING
   */
  @Variable(
      description =
          "A variable to configure the minimum allowed ratio between de- and inflated bytes to detect a zipbomb")
  public static final String HOP_ZIP_MIN_INFLATE_RATIO = "HOP_ZIP_MIN_INFLATE_RATIO";

  /**
   * The default value for the {@link #HOP_ZIP_MIN_INFLATE_RATIO} as a Double.
   *
   * @see #HOP_ZIP_MIN_INFLATE_RATIO
   * @see #HOP_ZIP_MIN_INFLATE_RATIO_DEFAULT_STRING
   */
  public static final Double HOP_ZIP_MIN_INFLATE_RATIO_DEFAULT = 0.01d;

  /**
   * The default value for the {@link #HOP_ZIP_MIN_INFLATE_RATIO} as a String.
   *
   * @see #HOP_ZIP_MIN_INFLATE_RATIO
   * @see #HOP_ZIP_MIN_INFLATE_RATIO_DEFAULT
   */
  @Variable(description = "")
  public static final String HOP_ZIP_MIN_INFLATE_RATIO_DEFAULT_STRING =
      String.valueOf(HOP_ZIP_MIN_INFLATE_RATIO_DEFAULT);

  /**
   * A variable to configure the maximum file size of a single zip entry.
   *
   * <p>If not set or if the configured value is invalid, it defaults to {@link
   * #HOP_ZIP_MAX_ENTRY_SIZE_DEFAULT}
   *
   * @see #HOP_ZIP_MAX_ENTRY_SIZE_DEFAULT
   * @see #HOP_ZIP_MAX_ENTRY_SIZE_DEFAULT_STRING
   */
  @Variable(description = "A variable to configure the maximum file size of a single zip entry")
  public static final String HOP_ZIP_MAX_ENTRY_SIZE = "HOP_ZIP_MAX_ENTRY_SIZE";

  /**
   * The default value for the {@link #HOP_ZIP_MAX_ENTRY_SIZE} as a Long.
   *
   * @see #HOP_ZIP_MAX_ENTRY_SIZE
   * @see #HOP_ZIP_MAX_ENTRY_SIZE_DEFAULT_STRING
   */
  public static final Long HOP_ZIP_MAX_ENTRY_SIZE_DEFAULT = 0xFFFFFFFFL;

  /**
   * The default value for the {@link #HOP_ZIP_MAX_ENTRY_SIZE} as a String.
   *
   * @see #HOP_ZIP_MAX_ENTRY_SIZE
   * @see #HOP_ZIP_MAX_ENTRY_SIZE_DEFAULT
   */
  @Variable(description = "")
  public static final String HOP_ZIP_MAX_ENTRY_SIZE_DEFAULT_STRING =
      String.valueOf(HOP_ZIP_MAX_ENTRY_SIZE_DEFAULT);

  /**
   * A variable to configure the maximum number of characters of text that are extracted before an
   * exception is thrown during extracting text from documents.
   *
   * <p>If not set or if the configured value is invalid, it defaults to {@link
   * #HOP_ZIP_MAX_TEXT_SIZE_DEFAULT}
   *
   * @see #HOP_ZIP_MAX_TEXT_SIZE_DEFAULT
   * @see #HOP_ZIP_MAX_TEXT_SIZE_DEFAULT_STRING
   */
  @Variable(
      description =
          "A variable to configure the maximum number of characters of text that are extracted before an exception is thrown during extracting text from documents")
  public static final String HOP_ZIP_MAX_TEXT_SIZE = "HOP_ZIP_MAX_TEXT_SIZE";

  /**
   * The default value for the {@link #HOP_ZIP_MAX_TEXT_SIZE} as a Long.
   *
   * @see #HOP_ZIP_MAX_TEXT_SIZE
   * @see #HOP_ZIP_MAX_TEXT_SIZE_DEFAULT_STRING
   */
  public static final Long HOP_ZIP_MAX_TEXT_SIZE_DEFAULT = 10 * 1024 * 1024L;

  /**
   * The default value for the {@link #HOP_ZIP_MAX_TEXT_SIZE} as a Long.
   *
   * @see #HOP_ZIP_MAX_TEXT_SIZE
   * @see #HOP_ZIP_MAX_TEXT_SIZE_DEFAULT
   */
  @Variable(description = "")
  public static final String HOP_ZIP_MAX_TEXT_SIZE_DEFAULT_STRING =
      String.valueOf(HOP_ZIP_MAX_TEXT_SIZE_DEFAULT);

  /**
   * This is the name of the variable which when set should contains the path to a file which will
   * be included in the serialization of pipelines and workflows.
   */
  @Variable(
      description =
          "This is the name of the variable which when set should contains the path to a file which will be included in the serialization of pipelines and workflows")
  public static final String HOP_LICENSE_HEADER_FILE = "HOP_LICENSE_HEADER_FILE";

  /** The variable says Hop to consider nulls when parsing JSON files */
  @Variable(
      value = "Y",
      description =
          "Name of the variable to set so that Nulls are considered while parsing JSON files. If HOP_JSON_INPUT_INCLUDE_NULLS is \"Y\" then nulls will be included (default behavior) otherwise they will not be included")
  public static final String HOP_JSON_INPUT_INCLUDE_NULLS = "HOP_JSON_INPUT_INCLUDE_NULLS";

  /** This variable is used to disable the strict searching of the context dialog */
  @Variable(
      value = "N",
      description =
          "This variable influences how the search is done in the context dialog, when set to Y it will do a strict search (Needed for automated UI testing)")
  public static final String HOP_CONTEXT_DIALOG_STRICT_SEARCH = "HOP_CONTEXT_DIALOG_STRICT_SEARCH";

  /** By default, HOP do consider NULLs while parsing input */
  public static final String JSON_INPUT_INCLUDE_NULLS = "Y";

  /** The i18n prefix to signal that this is a String in the format: i18n:package:key */
  public static final String I18N_PREFIX = "i18n:";

  /**
   * This is the name of the string used to store the connection group in pipelines and workflows.
   */
  public static final String CONNECTION_GROUP = "CONNECTION_GROUP";

  /**
   * This is the default wait time used for DynamicWaitTimes can be overwritten by a runtime
   * configuration
   */
  @Variable(
      scope = VariableScope.ENGINE,
      value = "20",
      description = "This is the default polling frequency for the transforms input buffer (in ms)")
  public static final String HOP_DEFAULT_BUFFER_POLLING_WAITTIME =
      "HOP_DEFAULT_BUFFER_POLLING_WAITTIME";

  /**
   * rounds double f to any number of places after decimal point Does arithmetic using BigDecimal
   * class to avoid integer overflow while rounding
   *
   * @param f The value to round
   * @param places The number of decimal places
   * @return The rounded floating point value
   */
  public static double round(double f, int places) {
    return round(f, places, RoundingMode.HALF_EVEN);
  }

  /**
   * @deprecated use {@link #round(double f, int places, RoundingMode roundingMode) round} instead
   *     rounds double f to any number of places after decimal point Does arithmetic using
   *     BigDecimal class to avoid integer overflow while rounding
   * @param f The value to round
   * @param places The number of decimal places
   * @param roundingMode The mode for rounding, e.g. java.math.BigDecimal.ROUND_HALF_EVEN
   * @return The rounded floating point value
   */
  @Deprecated(since = "2.10")
  public static double round(double f, int places, int roundingMode) {
    // We can't round non-numbers or infinite values
    //
    if (Double.isNaN(f) || f == Double.NEGATIVE_INFINITY || f == Double.POSITIVE_INFINITY) {
      return f;
    }

    // Do the rounding...
    //
    BigDecimal bdtemp = round(BigDecimal.valueOf(f), places, roundingMode);
    return bdtemp.doubleValue();
  }

  /**
   * rounds double f to any number of places after decimal point Does arithmetic using BigDecimal
   * class to avoid integer overflow while rounding
   *
   * @param f The value to round
   * @param places The number of decimal places
   * @param roundingMode The mode for rounding, e.g. RoundingMode.HALF_EVEN
   * @return The rounded floating point value
   */
  public static double round(double f, int places, RoundingMode roundingMode) {
    // We can't round non-numbers or infinite values
    //
    if (Double.isNaN(f) || f == Double.NEGATIVE_INFINITY || f == Double.POSITIVE_INFINITY) {
      return f;
    }

    // Do the rounding...
    //
    BigDecimal bdtemp = round(BigDecimal.valueOf(f), places, roundingMode);
    return bdtemp.doubleValue();
  }

  /**
   * @deprecated use {@link #round(BigDecimal f, int places, RoundingMode roundingMode) round}
   *     instead rounds BigDecimal f to any number of places after decimal point Does arithmetic
   *     using BigDecimal class to avoid integer overflow while rounding
   * @param f The value to round
   * @param places The number of decimal places
   * @param roundingMode The mode for rounding, e.g. java.math.BigDecimal.ROUND_HALF_EVEN
   * @return The rounded floating point value
   */
  @Deprecated(since = "2.10")
  @SuppressWarnings("java:S1874") // Ignore BigDecimal RoundingModes as code will be removed
  public static BigDecimal round(BigDecimal f, int places, int roundingMode) {
    if (roundingMode == ROUND_HALF_CEILING) {
      if (f.signum() >= 0) {
        return round(f, places, BigDecimal.ROUND_HALF_UP);
      } else {
        return round(f, places, BigDecimal.ROUND_HALF_DOWN);
      }
    } else {
      return f.setScale(places, roundingMode);
    }
  }

  /**
   * rounds BigDecimal f to any number of places after decimal point Does arithmetic using
   * BigDecimal class to avoid integer overflow while rounding
   *
   * @param f The value to round
   * @param places The number of decimal places
   * @param roundingMode The mode for rounding, e.g. java.math.BigDecimal.ROUND_HALF_EVEN
   * @return The rounded floating point value
   */
  public static BigDecimal round(BigDecimal f, int places, RoundingMode roundingMode) {
    if (roundingMode == null) {
      if (f.signum() >= 0) {
        return round(f, places, RoundingMode.HALF_UP);
      } else {
        return round(f, places, RoundingMode.HALF_DOWN);
      }
    } else {
      return f.setScale(places, roundingMode);
    }
  }

  /**
   * @deprecated use {@link #round(long f, int places, RoundingMode roundingMode) round} instead
   *     rounds long f to any number of places after decimal point Does arithmetic using BigDecimal
   *     class to avoid integer overflow while rounding
   * @param f The value to round
   * @param places The number of decimal places
   * @param roundingMode The mode for rounding, e.g. java.math.BigDecimal.ROUND_HALF_EVEN
   * @return The rounded floating point value
   */
  @Deprecated(since = "2.10")
  public static long round(long f, int places, int roundingMode) {
    if (places >= 0) {
      return f;
    }
    BigDecimal bdtemp = round(BigDecimal.valueOf(f), places, roundingMode);
    return bdtemp.longValue();
  }

  /**
   * rounds long f to any number of places after decimal point Does arithmetic using BigDecimal
   * class to avoid integer overflow while rounding
   *
   * @param f The value to round
   * @param places The number of decimal places
   * @param roundingMode The mode for rounding, e.g. java.math.BigDecimal.ROUND_HALF_EVEN
   * @return The rounded floating point value
   */
  public static long round(long f, int places, RoundingMode roundingMode) {
    if (places >= 0) {
      return f;
    }
    BigDecimal bdtemp = round(BigDecimal.valueOf(f), places, roundingMode);
    return bdtemp.longValue();
  }

  /**
   * Convert a String into an integer. If the conversion fails, assign a default value.
   *
   * @param str The String to convert to an integer
   * @param def The default value
   * @return The converted value or the default.
   */
  public static int toInt(String str, int def) {
    if (str == null) {
      return def;
    }
    int retval;
    try {
      retval = Integer.parseInt(str);
    } catch (Exception e) {
      retval = def;
    }
    return retval;
  }

  /**
   * Convert a String into a long integer. If the conversion fails, assign a default value.
   *
   * @param str The String to convert to a long integer
   * @param def The default value
   * @return The converted value or the default.
   */
  public static long toLong(String str, long def) {
    if (str == null) {
      return def;
    }
    long retval;
    try {
      retval = Long.parseLong(str);
    } catch (Exception e) {
      retval = def;
    }
    return retval;
  }

  /**
   * Convert a String into a double. If the conversion fails, assign a default value.
   *
   * @param str The String to convert to a double
   * @param def The default value
   * @return The converted value or the default.
   */
  public static double toDouble(String str, double def) {
    if (str == null) {
      return def;
    }
    double retval;
    try {
      retval = Double.parseDouble(str);
    } catch (Exception e) {
      retval = def;
    }
    return retval;
  }

  /**
   * Convert a String into a date. The date format is <code>yyyy/MM/dd HH:mm:ss.SSS</code>. If the
   * conversion fails, assign a default value.
   *
   * @param str The String to convert into a Date
   * @param def The default value
   * @return The converted value or the default.
   */
  public static Date toDate(String str, Date def) {
    if (str == null) {
      return def;
    }
    SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS", Locale.US);
    try {
      return df.parse(str);
    } catch (ParseException e) {
      return def;
    }
  }

  /**
   * Determines whether or not a character is considered a variables. A character is considered a
   * variables in Hop if it is a variables, a tab, a newline or a cariage return.
   *
   * @param c The character to verify if it is a variables.
   * @return true if the character is a variables. false otherwise.
   */
  public static boolean isSpace(char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n' || Character.isWhitespace(c);
  }

  /**
   * Left trim: remove spaces to the left of a String.
   *
   * @param source The String to left trim
   * @return The left trimmed String
   */
  public static String ltrim(String source) {
    if (source == null) {
      return null;
    }
    int from = 0;
    while (from < source.length() && isSpace(source.charAt(from))) {
      from++;
    }

    return source.substring(from);
  }

  /**
   * Right trim: remove spaces to the right of a string
   *
   * @param source The string to right trim
   * @return The trimmed string.
   */
  public static String rtrim(String source) {
    if (source == null) {
      return null;
    }

    int max = source.length();
    while (max > 0 && isSpace(source.charAt(max - 1))) {
      max--;
    }

    return source.substring(0, max);
  }

  /**
   * Trims a string: removes the leading and trailing spaces of a String.
   *
   * @param str The string to trim
   * @return The trimmed string.
   */
  public static String trim(String str) {
    if (str == null) {
      return null;
    }

    int max = str.length() - 1;
    int min = 0;

    while (min <= max && isSpace(str.charAt(min))) {
      min++;
    }
    while (max >= 0 && isSpace(str.charAt(max))) {
      max--;
    }

    if (max < min) {
      return "";
    }

    return str.substring(min, max + 1);
  }

  /**
   * Right pad a string: adds spaces to a string until a certain length. If the length is smaller
   * then the limit specified, the String is truncated.
   *
   * @param ret The string to pad
   * @param limit The desired length of the padded string.
   * @return The padded String.
   */
  public static String rightPad(String ret, int limit) {
    if (ret == null) {
      return rightPad(new StringBuilder(), limit);
    } else {
      return rightPad(new StringBuilder(ret), limit);
    }
  }

  /**
   * Right pad a StringBuffer: adds spaces to a string until a certain length. If the length is
   * smaller then the limit specified, the String is truncated.
   *
   * <p>MB - New version is nearly 25% faster
   *
   * @param ret The StringBuffer to pad
   * @param limit The desired length of the padded string.
   * @return The padded String.
   */
  public static String rightPad(StringBuffer ret, int limit) {
    if (ret != null) {
      while (ret.length() < limit) {
        ret.append("                    ");
      }
      ret.setLength(limit);
      return ret.toString();
    } else {
      return null;
    }
  }

  /**
   * Right pad a StringBuilder: adds spaces to a string until a certain length. If the length is
   * smaller then the limit specified, the String is truncated.
   *
   * <p>MB - New version is nearly 25% faster
   *
   * @param ret The StringBuilder to pad
   * @param limit The desired length of the padded string.
   * @return The padded String.
   */
  public static String rightPad(StringBuilder ret, int limit) {
    if (ret != null) {
      while (ret.length() < limit) {
        ret.append("                    ");
      }
      ret.setLength(limit);
      return ret.toString();
    } else {
      return null;
    }
  }

  /**
   * Replace values in a String with another.
   *
   * <p>33% Faster using replaceAll this way than original method
   *
   * @param string The original String.
   * @param repl The text to replace
   * @param with The new text bit
   * @return The resulting string with the text pieces replaced.
   */
  public static String replace(String string, String repl, String with) {
    if (string != null && repl != null && with != null) {
      return string.replaceAll(Pattern.quote(repl), Matcher.quoteReplacement(with));
    } else {
      return null;
    }
  }

  /**
   * Alternate faster version of string replace using a stringbuffer as input.
   *
   * <p>33% Faster using replaceAll this way than original method
   *
   * @param str The string where we want to replace in
   * @param code The code to search for
   * @param repl The replacement string for code
   */
  public static void repl(StringBuffer str, String code, String repl) {
    if ((code == null)
        || (repl == null)
        || (code.length() == 0)
        || (repl.length() == 0)
        || (str == null)
        || (str.length() == 0)) {
      return; // do nothing
    }
    String aString = str.toString();
    str.setLength(0);
    str.append(aString.replaceAll(Pattern.quote(code), Matcher.quoteReplacement(repl)));
  }

  /**
   * Alternate faster version of string replace using a stringbuilder as input (non-synchronized).
   *
   * <p>33% Faster using replaceAll this way than original method
   *
   * @param str The string where we want to replace in
   * @param code The code to search for
   * @param repl The replacement string for code
   */
  public static void repl(StringBuilder str, String code, String repl) {
    if ((code == null) || (repl == null) || (str == null)) {
      return; // do nothing
    }
    String aString = str.toString();
    str.setLength(0);
    str.append(aString.replaceAll(Pattern.quote(code), Matcher.quoteReplacement(repl)));
  }

  /**
   * Count the number of spaces to the left of a text. (leading)
   *
   * @param field The text to examine
   * @return The number of leading spaces found.
   */
  public static int nrSpacesBefore(String field) {
    int nr = 0;
    int len = field.length();
    while (nr < len && field.charAt(nr) == ' ') {
      nr++;
    }
    return nr;
  }

  /**
   * Count the number of spaces to the right of a text. (trailing)
   *
   * @param field The text to examine
   * @return The number of trailing spaces found.
   */
  public static int nrSpacesAfter(String field) {
    int nr = 0;
    int len = field.length();
    while (nr < len && field.charAt(field.length() - 1 - nr) == ' ') {
      nr++;
    }
    return nr;
  }

  /**
   * Checks whether or not a String consists only of spaces.
   *
   * @param str The string to check
   * @return true if the string has nothing but spaces.
   */
  public static boolean onlySpaces(String str) {
    for (int i = 0; i < str.length(); i++) {
      if (!isSpace(str.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return the operating system hop runs on
   */
  public static String getSystemOs() {
    return System.getProperty("os.name");
  }

  /**
   * This is determined by the HOP_PLATFORM_OS variable set by the Hop execution script(s)
   *
   * @return The name of the Hop tool running right now
   */
  public static String getHopPlatformOs() {
    return System.getProperty(HOP_PLATFORM_OS, "");
  }

  /**
   * This is determined by the HOP_PLATFORM_RUNTIME variable set by the Hop execution script(s)
   *
   * @return The name of the Hop tool running right now
   */
  public static String getHopPlatformRuntime() {
    return System.getProperty(HOP_PLATFORM_RUNTIME);
  }

  /**
   * Determine the quoting character depending on the OS. Often used for shell calls, gives back "
   * for Windows systems otherwise '
   *
   * @return quoting character
   */
  public static String getQuoteCharByOS() {
    if (isWindows()) {
      return "\"";
    } else {
      return "'";
    }
  }

  /**
   * Quote a string depending on the OS. Often used for shell calls.
   *
   * @return quoted string
   */
  public static String optionallyQuoteStringByOS(String string) {
    String quote = getQuoteCharByOS();
    if (Utils.isEmpty(string)) {
      return quote;
    }

    // If the field already contains quotes, we don't touch it anymore, just
    // return the same string...
    // also return it if no spaces are found
    if (string.contains(quote) || (string.indexOf(' ') < 0 && string.indexOf('=') < 0)) {
      return string;
    } else {
      return quote + string + quote;
    }
  }

  /**
   * @return True if the OS is a Windows derivate.
   */
  public static boolean isWindows() {
    return getHopPlatformOs().startsWith(CONST_WINDOWS) || getSystemOs().startsWith(CONST_WINDOWS);
  }

  /**
   * @return True if the OS is a Linux derivate.
   */
  public static boolean isLinux() {
    return getHopPlatformOs().startsWith(CONST_LINUX) || getSystemOs().startsWith(CONST_LINUX);
  }

  /**
   * @return True if the OS is an OSX derivate.
   */
  public static boolean isOSX() {
    return getHopPlatformOs().startsWith("Darwin") || getSystemOs().toUpperCase().contains("OS X");
  }

  /**
   * @return True if KDE is in use.
   */
  public static boolean isKDE() {
    return StringUtils.isNotBlank(System.getenv("KDE_SESSION_VERSION"));
  }

  private static String cachedHostname;

  /**
   * Determine the hostname of the machine Hop is running on
   *
   * @return The hostname
   */
  public static String getHostname() {

    if (cachedHostname != null) {
      return cachedHostname;
    }

    // In case we don't want to leave anything to doubt...
    //
    String systemHostname = EnvUtil.getSystemProperty(HOP_SYSTEM_HOSTNAME);
    if (!Utils.isEmpty(systemHostname)) {
      cachedHostname = systemHostname;
      return systemHostname;
    }

    String lastHostname = "localhost";
    try {
      Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
      while (en.hasMoreElements()) {
        NetworkInterface nwi = en.nextElement();
        Enumeration<InetAddress> ip = nwi.getInetAddresses();

        while (ip.hasMoreElements()) {
          InetAddress in = ip.nextElement();
          lastHostname = in.getHostName();
          if (!lastHostname.equalsIgnoreCase("localhost") && !(lastHostname.indexOf(':') >= 0)) {
            break;
          }
        }
      }
    } catch (SocketException e) {
      // Eat exception, just return what you have
    }

    cachedHostname = lastHostname;

    return lastHostname;
  }

  /**
   * Determine the hostname of the machine Hop is running on
   *
   * @return The hostname
   */
  public static String getHostnameReal() {

    // In case we don't want to leave anything to doubt...
    //
    String systemHostname = EnvUtil.getSystemProperty(HOP_SYSTEM_HOSTNAME);
    if (!Utils.isEmpty(systemHostname)) {
      return systemHostname;
    }

    if (isWindows()) {
      // Windows will always set the 'COMPUTERNAME' variable
      return System.getenv("COMPUTERNAME");
    } else {
      // If it is not Windows then it is most likely a Unix-like operating system
      // such as Solaris, AIX, HP-UX, Linux or MacOS.
      // Most modern shells (such as Bash or derivatives) sets the
      // HOSTNAME variable so lets try that first.
      String hostname = System.getenv("HOSTNAME");
      if (hostname != null) {
        return hostname;
      } else {
        BufferedReader br;
        try {
          Process pr = Runtime.getRuntime().exec("hostname");
          br = new BufferedReader(new InputStreamReader(pr.getInputStream()));
          String line;
          if ((line = br.readLine()) != null) {
            return line;
          }
          pr.waitFor();
          br.close();
        } catch (IOException | InterruptedException e) {
          return getHostname();
        }
      }
    }
    return getHostname();
  }

  /**
   * Determins the IP address of the machine Hop is running on.
   *
   * @return The IP address
   */
  public static String getIPAddress() throws Exception {
    Enumeration<NetworkInterface> enumInterfaces = NetworkInterface.getNetworkInterfaces();
    while (enumInterfaces.hasMoreElements()) {
      NetworkInterface nwi = enumInterfaces.nextElement();
      Enumeration<InetAddress> ip = nwi.getInetAddresses();
      while (ip.hasMoreElements()) {
        InetAddress in = ip.nextElement();
        if (!in.isLoopbackAddress() && in.toString().indexOf(':') < 0) {
          return in.getHostAddress();
        }
      }
    }
    return "127.0.0.1";
  }

  /**
   * Get the primary IP address tied to a network interface (excluding loop-back etc)
   *
   * @param networkInterfaceName the name of the network interface to interrogate
   * @return null if the network interface or address wasn't found.
   * @throws SocketException in case of a security or network error
   */
  public static String getIPAddress(String networkInterfaceName) throws SocketException {
    NetworkInterface networkInterface = NetworkInterface.getByName(networkInterfaceName);
    Enumeration<InetAddress> ipAddresses = networkInterface.getInetAddresses();
    while (ipAddresses.hasMoreElements()) {
      InetAddress inetAddress = ipAddresses.nextElement();
      if (!inetAddress.isLoopbackAddress() && inetAddress.toString().indexOf(':') < 0) {
        return inetAddress.getHostAddress();
      }
    }
    return null;
  }

  /**
   * Tries to determine the MAC address of the machine Hop is running on.
   *
   * @return The MAC address.
   */
  public static String getMACAddress() throws Exception {
    String ip = getIPAddress();
    String mac = "none";
    String os = getSystemOs();
    String s = "";
    boolean errorOccured = false;
    if (os.equalsIgnoreCase("Windows NT")
        || os.equalsIgnoreCase("Windows 2000")
        || os.equalsIgnoreCase("Windows XP")
        || os.equalsIgnoreCase("Windows 95")
        || os.equalsIgnoreCase("Windows 98")
        || os.equalsIgnoreCase("Windows Me")
        || os.startsWith(CONST_WINDOWS)) {
      try {
        Process p = Runtime.getRuntime().exec("nbtstat -a " + ip);

        // read the standard output of the command
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));

        while (!procDone(p)) {
          while ((s = stdInput.readLine()) != null) {
            if (s.contains("MAC")) {
              int idx = s.indexOf('=');
              mac = s.substring(idx + 2);
            }
          }
        }
        stdInput.close();
      } catch (Exception e) {
        errorOccured = true;
      }
    } else if (os.equalsIgnoreCase(CONST_LINUX)) {
      try {
        Process p = Runtime.getRuntime().exec("/sbin/ifconfig -a");

        // read the standard output of the command
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));

        while (!procDone(p)) {
          while ((s = stdInput.readLine()) != null) {
            int idx = s.indexOf("HWaddr");
            if (idx >= 0) {
              mac = s.substring(idx + 7);
            }
          }
        }
        stdInput.close();
      } catch (Exception e) {
        errorOccured = true;
      }
    } else if (os.equalsIgnoreCase("Solaris")) {
      try {
        Process p = Runtime.getRuntime().exec("/usr/sbin/ifconfig -a");

        // read the standard output of the command
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));

        while (!procDone(p)) {
          while ((s = stdInput.readLine()) != null) {
            int idx = s.indexOf("ether");
            if (idx >= 0) {
              mac = s.substring(idx + 6);
            }
          }
        }
        stdInput.close();
      } catch (Exception e) {
        errorOccured = true;
      }
    } else if (os.equalsIgnoreCase("HP-UX")) {
      try {
        Process p = Runtime.getRuntime().exec("/usr/sbin/lanscan -a");

        // read the standard output of the command
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));

        while (!procDone(p)) {
          while ((s = stdInput.readLine()) != null) {
            if (s.contains("MAC")) {
              int idx = s.indexOf("0x");
              mac = s.substring(idx + 2);
            }
          }
        }
        stdInput.close();
      } catch (Exception e) {
        errorOccured = true;
      }
    }
    // should do something if we got an error processing!
    return Const.trim(mac);
  }

  private static final boolean procDone(Process p) {
    try {
      p.exitValue();
      return true;
    } catch (IllegalThreadStateException e) {
      return false;
    }
  }

  /**
   * Returns the path to the Hop local (current directory) Hop Server password file:
   *
   * <p>./pwd/hop.pwd<br>
   *
   * @return The local hop server password file.
   */
  public static String getHopLocalServerPasswordFile() {
    return "pwd/hop.pwd";
  }

  /**
   * Provides the base documentation url (top-level help)
   *
   * @return the fully qualified base documentation URL
   */
  public static String getBaseDocUrl() {
    String url = BaseMessages.getString(PKG, "Const.BaseDocUrl");

    // Get the implementation version:
    // Temporary build: 2.4.0-SNAPSHOT (2023-02-13 08.50.52)
    // Release version: 2.4.0
    String version = Const.class.getPackage().getImplementationVersion();

    // Check if implementation version is a SNAPHOT build or if version is not known.
    if (version == null || version.contains("SNAPSHOT")) {
      version = "next";
    } else {
      // Only keep until first space to remove the build date
      version = version.split(" ")[0];
    }

    return url + version + "/";
  }

  /**
   * Provides the documentation url with the configured base + the given URI.
   *
   * @param uri the resource identifier for the documentation (eg. pipeline/transforms/abort.html)
   * @return the fully qualified documentation URL for the given URI
   */
  public static String getDocUrl(final String uri) {
    // initialize the docUrl to point to the top-level doc page
    String docUrl = getBaseDocUrl();
    if (!Utils.isEmpty(uri)) {
      // if the uri is not empty, use it to build the URL
      if (uri.startsWith("http")) {
        // use what is provided, it's already absolute
        docUrl = uri;
      } else {
        // the uri provided needs to be assembled
        docUrl = uri.startsWith("/") ? docUrl + uri.substring(1) : docUrl + uri;
      }
    }
    return docUrl;
  }

  /**
   * Retrieves the content of an environment variable
   *
   * @param variable The name of the environment variable
   * @param deflt The default value in case no value was found
   * @return The value of the environment variable or the value of deflt in case no variable was
   *     defined.
   */
  public static String getEnvironmentVariable(String variable, String deflt) {
    return System.getProperty(variable, deflt);
  }

  /**
   * Implements Oracle style NVL function
   *
   * @param source The source argument
   * @param def The default value in case source is null or the length of the string is 0
   * @return source if source is not null, otherwise return def
   */
  public static String NVL(String source, String def) {
    if (source == null || source.length() == 0) {
      return def;
    }
    return source;
  }

  /**
   * Return empty string "" in case the given parameter is null, otherwise return the same value.
   *
   * @param source The source value to check for null.
   * @return empty string if source is null, otherwise simply return the source value.
   */
  public static String nullToEmpty(String source) {
    if (source == null) {
      return "";
    }
    return source;
  }

  /**
   * Search for a string in an array of strings and return the index.
   *
   * @param lookup The string to search for
   * @param array The array of strings to look in
   * @return The index of a search string in an array of strings. -1 if not found.
   */
  public static int indexOfString(String lookup, String[] array) {
    if (array == null) {
      return -1;
    }
    if (lookup == null) {
      return -1;
    }

    for (int i = 0; i < array.length; i++) {
      if (lookup.equalsIgnoreCase(array[i])) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Search for strings in an array of strings and return the indexes.
   *
   * @param lookup The strings to search for
   * @param array The array of strings to look in
   * @return The indexes of strings in an array of strings. -1 if not found.
   */
  public static int[] indexsOfStrings(String[] lookup, String[] array) {
    int[] indexes = new int[lookup.length];
    for (int i = 0; i < indexes.length; i++) {
      indexes[i] = indexOfString(lookup[i], array);
    }
    return indexes;
  }

  /**
   * Search for strings in an array of strings and return the indexes. If a string is not found, the
   * index is not returned.
   *
   * @param lookup The strings to search for
   * @param array The array of strings to look in
   * @return The indexes of strings in an array of strings. Only existing indexes are returned (no
   *     -1)
   */
  public static int[] indexesOfFoundStrings(String[] lookup, String[] array) {
    List<Integer> indexesList = new ArrayList<>();
    for (int i = 0; i < lookup.length; i++) {
      int idx = indexOfString(lookup[i], array);
      if (idx >= 0) {
        indexesList.add(Integer.valueOf(idx));
      }
    }
    int[] indexes = new int[indexesList.size()];
    for (int i = 0; i < indexesList.size(); i++) {
      indexes[i] = (indexesList.get(i)).intValue();
    }
    return indexes;
  }

  /**
   * Search for strings in a list of strings and return the indexes. If a string is not found, the
   * index is not returned.
   *
   * @param lookup The strings to search for
   * @param list The array of strings to look in
   * @return The indexes of strings in a list of strings. Only existing indexes are returned (no -1)
   */
  public static List<Integer> indexesOfFoundStrings(List<String> lookup, List<String> list) {
    List<Integer> indexesList = new ArrayList<>();
    for (int i = 0; i < lookup.size(); i++) {
      int idx = indexOfString(lookup.get(i), list);
      if (idx >= 0) {
        indexesList.add(Integer.valueOf(idx));
      }
    }
    int[] indexes = new int[indexesList.size()];
    for (int i = 0; i < indexesList.size(); i++) {
      indexes[i] = (indexesList.get(i)).intValue();
    }
    return indexesList;
  }

  /**
   * Search for a string in a list of strings and return the index.
   *
   * @param lookup The string to search for
   * @param list The ArrayList of strings to look in
   * @return The index of a search string in an array of strings. -1 if not found.
   */
  public static int indexOfString(String lookup, List<String> list) {
    if (list == null) {
      return -1;
    }

    for (int i = 0; i < list.size(); i++) {
      String compare = list.get(i);
      if (lookup.equalsIgnoreCase(compare)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Sort the strings of an array in alphabetical order.
   *
   * @param input The array of strings to sort.
   * @return The sorted array of strings.
   */
  public static String[] sortStrings(String[] input) {
    Arrays.sort(input);
    return input;
  }

  /**
   * Convert strings separated by a string into an array of strings.
   *
   * <p><code>
   * Example: a;b;c;d    ==>    new String[] { a, b, c, d }
   * </code>
   *
   * <p><b>NOTE: this differs from String.split() in a way that the built-in method uses regular
   * expressions and this one does not.</b>
   *
   * @param string The string to split
   * @param separator The separator used.
   * @return the string split into an array of strings
   */
  public static String[] splitString(String string, String separator) {
    /*
     * 0123456 Example a;b;c;d --> new String[] { a, b, c, d }
     */
    List<String> list = new ArrayList<>();

    if (string == null || string.length() == 0) {
      return new String[] {};
    }

    int sepLen = separator.length();
    int from = 0;
    int end = string.length() - sepLen + 1;

    for (int i = from; i < end; i += sepLen) {
      if (string.substring(i, i + sepLen).equalsIgnoreCase(separator)) {
        // OK, we found a separator, the string to add to the list
        // is [from, i[
        list.add(nullToEmpty(string.substring(from, i)));
        from = i + sepLen;
      }
    }

    // Wait, if the string didn't end with a separator, we still have information at the end of the
    // string...
    // In our example that would be "d"...
    if (from + sepLen <= string.length()) {
      list.add(nullToEmpty(string.substring(from)));
    }

    return list.toArray(new String[list.size()]);
  }

  /**
   * Convert strings separated by a character into an array of strings.
   *
   * <p><code>
   * Example: a;b;c;d    ==  new String[] { a, b, c, d }
   * </code>
   *
   * @param string The string to split
   * @param separator The separator used.
   * @return the string split into an array of strings
   */
  public static String[] splitString(String string, char separator) {
    return splitString(string, separator, false);
  }

  /**
   * Convert strings separated by a character into an array of strings.
   *
   * <p><code>
   * Example: a;b;c;d    ==  new String[] { a, b, c, d }
   * </code>
   *
   * @param string The string to split
   * @param separator The separator used.
   * @param escape in case the separator can be escaped (\;) The escape characters are NOT removed!
   * @return the string split into an array of strings
   */
  public static String[] splitString(String string, char separator, boolean escape) {
    /*
     * 0123456 Example a;b;c;d --> new String[] { a, b, c, d }
     */
    List<String> list = new ArrayList<>();

    if (string == null || string.length() == 0) {
      return new String[] {};
    }

    int from = 0;
    int end = string.length();

    for (int i = from; i < end; i += 1) {
      boolean found = string.charAt(i) == separator;
      if (found && escape && i > 0) {
        found &= string.charAt(i - 1) != '\\';
      }
      if (found) {
        // OK, we found a separator, the string to add to the list
        // is [from, i[
        list.add(nullToEmpty(string.substring(from, i)));
        from = i + 1;
      }
    }

    // Wait, if the string didn't end with a separator, we still have information at the end of the
    // string...
    // In our example that would be "d"...
    if (from + 1 <= string.length()) {
      list.add(nullToEmpty(string.substring(from)));
    }

    return list.toArray(new String[list.size()]);
  }

  /**
   * Convert strings separated by a string into an array of strings.
   *
   * <p><code>
   * Example /a/b/c --> new String[] { a, b, c }
   * </code>
   *
   * @param path The string to split
   * @param separator The separator used.
   * @return the string split into an array of strings
   */
  public static String[] splitPath(String path, String separator) {
    //
    // Example /a/b/c --> new String[] { a, b, c }
    //
    // Make sure training slashes are removed
    //
    // Example /a/b/c/ --> new String[] { a, b, c }
    //

    // Check for empty paths...
    //
    if (path == null || path.length() == 0 || path.equals(separator)) {
      return new String[] {};
    }

    // lose trailing separators
    //
    while (path.endsWith(separator)) {
      path = path.substring(0, path.length() - 1);
    }

    int sepLen = separator.length();
    int nrSeparators = 1;
    int from = path.startsWith(separator) ? sepLen : 0;

    for (int i = from; i < path.length(); i += sepLen) {
      if (path.substring(i, i + sepLen).equalsIgnoreCase(separator)) {
        nrSeparators++;
      }
    }

    String[] spath = new String[nrSeparators];
    int nr = 0;
    for (int i = from; i < path.length(); i += sepLen) {
      if (path.substring(i, i + sepLen).equalsIgnoreCase(separator)) {
        spath[nr] = path.substring(from, i);
        nr++;

        from = i + sepLen;
      }
    }
    if (nr < spath.length) {
      spath[nr] = path.substring(from);
    }

    //
    // a --> { a }
    //
    if (spath.length == 0 && path.length() > 0) {
      spath = new String[] {path};
    }

    return spath;
  }

  /**
   * Split the given string using the given delimiter and enclosure strings.
   *
   * <p>The delimiter and enclosures are not regular expressions (regexes); rather they are literal
   * strings that will be quoted so as not to be treated like regexes.
   *
   * <p>This method expects that the data contains an even number of enclosure strings in the input;
   * otherwise the results are undefined
   *
   * @param stringToSplit the String to split
   * @param delimiter the delimiter string
   * @param enclosure the enclosure string
   * @return an array of strings split on the delimiter (ignoring those in enclosures), or null if
   *     the string to split is null.
   */
  public static String[] splitString(String stringToSplit, String delimiter, String enclosure) {
    return splitString(stringToSplit, delimiter, enclosure, false);
  }

  /**
   * Split the given string using the given delimiter and enclosure strings.
   *
   * <p>The delimiter and enclosures are not regular expressions (regexes); rather they are literal
   * strings that will be quoted so as not to be treated like regexes.
   *
   * <p>This method expects that the data contains an even number of enclosure strings in the input;
   * otherwise the results are undefined
   *
   * @param stringToSplit the String to split
   * @param delimiter the delimiter string
   * @param enclosure the enclosure string
   * @param removeEnclosure removes enclosure from split result
   * @return an array of strings split on the delimiter (ignoring those in enclosures), or null if
   *     the string to split is null.
   */
  public static String[] splitString(
      String stringToSplit, String delimiter, String enclosure, boolean removeEnclosure) {
    return splitString(stringToSplit, delimiter, enclosure, removeEnclosure, null);
  }

  /**
   * Split the given string using the given delimiter and enclosure strings.
   *
   * <p>The delimiter and enclosures are not regular expressions (regexes); rather they are literal
   * strings that will be quoted so as not to be treated like regexes.
   *
   * <p>This method expects that the data contains an even number of enclosure strings in the input;
   * otherwise the results are undefined
   *
   * @param stringToSplit the String to split
   * @param delimiter the delimiter string
   * @param enclosure the enclosure string
   * @param removeEnclosure removes enclosure from split result
   * @param escape The escape string which will ignore a delimited if it precedes that.
   * @return an array of strings split on the delimiter (ignoring those in enclosures), or null if
   *     the string to split is null.
   */
  public static String[] splitString(
      String stringToSplit,
      String delimiter,
      String enclosure,
      boolean removeEnclosure,
      String escape) {

    ArrayList<String> splitList = null;
    boolean withEnclosure = StringUtils.isNotEmpty(enclosure);
    boolean withEscape = StringUtils.isNotEmpty(escape);
    boolean concatWithNext = false;

    // Handle "bad input" cases
    if (stringToSplit == null) {
      return null;
    }
    if (delimiter == null) {
      return (new String[] {stringToSplit});
    }

    // Split the string on the delimiter, we'll build the "real" results from the partial results
    String[] delimiterSplit = stringToSplit.split(Pattern.quote(delimiter));

    // Keep track of partial splits and concatenate them into a legit split
    StringBuilder concatSplit = null;

    if (delimiterSplit != null && delimiterSplit.length > 0) {

      // We'll have at least one result so create the result list object
      splitList = new ArrayList<>();

      // Proceed through the partial splits, concatenating if the splits are within the enclosure
      for (String currentSplit : delimiterSplit) {
        if (withEnclosure && !currentSplit.contains(enclosure)) {

          // If we are currently concatenating a split, we are inside an enclosure. Since this
          // split doesn't contain an enclosure, we can concatenate it (with a delimiter in front).
          // If we're not concatenating, the split is fine so add it to the result list.
          if (concatSplit != null) {
            concatSplit.append(delimiter);
            concatSplit.append(currentSplit);
          } else {
            splitList.add(currentSplit);
          }
        } else {

          boolean addSplit = false;

          if (withEscape) {

            if (concatWithNext) {
              concatWithNext = false;
              addSplit = true;
            }

            if (currentSplit.equals(escape)) {
              // We split a string with an escaped delimiter at the front or back:
              //   aaaaa:bbbb:\\:cccc ==> aaaa,bbbb,\\,cccc
              //   aaaaa:bbbb:cccc\\: ==> aaaa,bbbb,cccc,\\
              //
              // So in this case we simply set the current split to the delimiter
              //
              currentSplit = delimiter;
              concatWithNext = true;
            } else if (currentSplit.endsWith(escape)) {
              // We split a string like   aaaaa:bbbb:cc\\:cc
              // This gave us aaaa,bbbb,cc\\,cc
              // This covers the first part of cccc : cc\\
              // So we need to remove the escape and replace it with the delimiter
              //
              currentSplit =
                  currentSplit.substring(0, currentSplit.length() - escape.length()) + delimiter;
              concatWithNext = true;
            }

            if (addSplit) {
              int lastSplitIndex = splitList.size() - 1;
              currentSplit = splitList.get(lastSplitIndex) + currentSplit;
              splitList.remove(lastSplitIndex);
            }
          }

          // Find number of enclosures in the split, and whether that number is odd or even.
          int numEnclosures = withEnclosure ? StringUtils.countMatches(currentSplit, enclosure) : 0;
          boolean oddNumberOfEnclosures = (numEnclosures % 2 != 0);

          // This split contains an enclosure, so either start or finish concatenating
          if (concatSplit == null) {
            concatSplit = new StringBuilder(currentSplit); // start concatenation
            addSplit |= !oddNumberOfEnclosures;
          } else {
            // Check to make sure a new enclosure hasn't started within this split. This method
            // expects
            // that there are no non-delimiter characters between a delimiter and a starting
            // enclosure.

            // At this point in the code, the split shouldn't start with the enclosure, so add a
            // delimiter
            concatSplit.append(delimiter);

            // Add the current split to the concatenated split
            concatSplit.append(currentSplit);

            // If the number of enclosures is odd, the enclosure is closed so add the split to the
            // list
            // and reset the "concatSplit" buffer. Otherwise continue
            addSplit = oddNumberOfEnclosures;
          }
          if (addSplit) {
            String splitResult = concatSplit.toString();
            // remove enclosure from resulting split
            if (withEnclosure && removeEnclosure) {
              splitResult = removeEnclosure(splitResult, enclosure);
            }

            splitList.add(splitResult);
            concatSplit = null;
            addSplit = false;
          }
        }
      }
    }

    // Return list as array
    return splitList != null ? splitList.toArray(new String[splitList.size()]) : new String[0];
  }

  private static String removeEnclosure(String stringToSplit, String enclosure) {
    int firstIndex = stringToSplit.indexOf(enclosure);
    int lastIndex = stringToSplit.lastIndexOf(enclosure);
    if (firstIndex == lastIndex) {
      return stringToSplit;
    }
    StrBuilder strBuilder = new StrBuilder(stringToSplit);
    strBuilder.replace(firstIndex, enclosure.length() + firstIndex, "");
    strBuilder.replace(lastIndex - enclosure.length(), lastIndex, "");

    return strBuilder.toString();
  }

  /**
   * Sorts the array of Strings, determines the uniquely occurring strings.
   *
   * @param strings the array that you want to do a distinct on
   * @return a sorted array of uniquely occurring strings
   */
  public static String[] getDistinctStrings(String[] strings) {
    if (strings == null) {
      return null;
    }
    if (strings.length == 0) {
      return new String[] {};
    }
    HashSet<String> set = new HashSet<>();
    Collections.addAll(set, strings);
    List<String> list = new ArrayList<>(set);
    Collections.sort(list);
    return list.toArray(new String[0]);
  }

  /** Returns a string of the stack trace of the specified exception */
  public static String getStackTracker(Throwable e) {
    if (isUsingSimpleStackTraces()) {
      return getSimpleStackTrace(e);
    } else {
      return getClassicStackTrace(e);
    }
  }

  public static String getClassicStackTrace(Throwable e) {
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    e.printStackTrace(printWriter);
    String string = stringWriter.toString();
    try {
      stringWriter.close();
    } catch (IOException ioe) {
      // is this really required?
    }
    return string;
  }

  public static String getSimpleStackTrace(Throwable aThrowable) {
    return ExceptionUtils.getMessage(aThrowable)
        + Const.CR
        + "Root cause: "
        + ExceptionUtils.getRootCauseMessage(aThrowable);
  }

  /**
   * Create a valid filename using a name We remove all special characters, spaces, etc.
   *
   * @param name The name to use as a base for the filename
   * @return a valid filename
   */
  public static String createFilename(String name) {
    StringBuilder filename = new StringBuilder();
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if (Character.isUnicodeIdentifierPart(c)) {
        filename.append(c);
      } else if (Character.isWhitespace(c)) {
        filename.append('_');
      }
    }
    return filename.toString().toLowerCase();
  }

  public static String createFilename(String directory, String name, String extension) {
    if (directory.endsWith(Const.FILE_SEPARATOR)) {
      return directory + createFilename(name) + extension;
    } else {
      return directory + Const.FILE_SEPARATOR + createFilename(name) + extension;
    }
  }

  public static String createName(String filename) {
    if (Utils.isEmpty(filename)) {
      return filename;
    }

    String pureFilename = filenameOnly(filename);
    if (pureFilename.endsWith(".hpl")
        || pureFilename.endsWith(".hwf")
        || pureFilename.endsWith(".xml")) {
      pureFilename = pureFilename.substring(0, pureFilename.length() - 4);
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < pureFilename.length(); i++) {
      char c = pureFilename.charAt(i);
      if (Character.isUnicodeIdentifierPart(c)) {
        sb.append(c);
      } else if (Character.isWhitespace(c)) {
        sb.append(' ');
      } else if (c == '-') {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  /**
   * Returns the pure filename of a filename with full path. E.g. if passed parameter is <code>
   * /opt/tomcat/logs/catalina.out</code> this method returns <code>catalina.out</code>. The method
   * works with the Environment variable <i>System.getProperty("file.separator")</i>, so on
   * linux/Unix it will check for the last occurrence of a frontslash, on windows for the last
   * occurrence of a backslash.
   *
   * <p>To make this OS independent, the method could check for the last occurrence of a frontslash
   * and backslash and use the higher value of both. Should work, since these characters aren't
   * allowed in filenames on neither OS types (or said differently: Neither linux nor windows can
   * carry frontslashes OR backslashes in filenames). Just a suggestion of an improvement ...
   *
   * @param sFullPath
   * @return
   */
  public static String filenameOnly(String sFullPath) {
    if (Utils.isEmpty(sFullPath)) {
      return sFullPath;
    }

    int idx = sFullPath.lastIndexOf(FILE_SEPARATOR);
    if (idx != -1) {
      return sFullPath.substring(idx + 1);
    } else {
      idx = sFullPath.lastIndexOf('/'); // URL, VFS/**/
      if (idx != -1) {
        return sFullPath.substring(idx + 1);
      } else {
        return sFullPath;
      }
    }
  }

  /**
   * Returning the localized date conversion formats. They get created once on first request.
   *
   * @return
   */
  public static String[] getDateFormats() {
    if (dateFormats == null) {
      int dateFormatsCount = toInt(BaseMessages.getString(PKG, "Const.DateFormat.Count"), 0);
      dateFormats = new String[dateFormatsCount];
      for (int i = 1; i <= dateFormatsCount; i++) {
        dateFormats[i - 1] = BaseMessages.getString(PKG, "Const.DateFormat" + i);
      }
      Arrays.sort(dateFormats);
    }
    return dateFormats;
  }

  /**
   * Returning the localized number conversion formats. They get created once on first request.
   *
   * @return
   */
  public static String[] getNumberFormats() {
    if (numberFormats == null) {
      int numberFormatsCount = toInt(BaseMessages.getString(PKG, "Const.NumberFormat.Count"), 0);
      numberFormats = new String[numberFormatsCount + 1];
      numberFormats[0] = DEFAULT_NUMBER_FORMAT;
      for (int i = 1; i <= numberFormatsCount; i++) {
        numberFormats[i] = BaseMessages.getString(PKG, "Const.NumberFormat" + i);
      }
    }
    return numberFormats;
  }

  /**
   * @return An array of all default conversion formats, to be used in dialogs etc.
   */
  public static String[] getConversionFormats() {
    return (String[]) ArrayUtils.addAll(Const.getDateFormats(), Const.getNumberFormats());
  }

  /**
   * Return the input string trimmed as specified.
   *
   * @param string String to be trimmed
   * @param trimType Type of trimming
   * @return Trimmed string.
   */
  public static String trimToType(String string, int trimType) {
    switch (trimType) {
      case IValueMeta.TRIM_TYPE_BOTH:
        return trim(string);
      case IValueMeta.TRIM_TYPE_LEFT:
        return ltrim(string);
      case IValueMeta.TRIM_TYPE_RIGHT:
        return rtrim(string);
      case IValueMeta.TRIM_TYPE_NONE:
      default:
        return string;
    }
  }

  /**
   * implemented to help prevent errors in matching up pluggable LAF directories and paths/files
   * eliminating malformed URLs - duplicate file separators or missing file separators.
   *
   * @param dir
   * @param file
   * @return concatenated string representing a file url
   */
  public static String safeAppendDirectory(String dir, String file) {
    boolean dirHasSeparator = ((dir.lastIndexOf(FILE_SEPARATOR)) == dir.length() - 1);
    boolean fileHasSeparator = (file.indexOf(FILE_SEPARATOR) == 0);
    if ((dirHasSeparator && !fileHasSeparator) || (!dirHasSeparator && fileHasSeparator)) {
      return dir + file;
    }
    if (dirHasSeparator && fileHasSeparator) {
      return dir + file.substring(1);
    }
    return dir + FILE_SEPARATOR + file;
  }

  /**
   * Create an array of Strings consisting of spaces. The index of a String in the array determines
   * the number of spaces in that string.
   *
   * @return array of 'variables' Strings.
   */
  public static String[] getEmptyPaddedStrings() {
    if (emptyPaddedSpacesStrings == null) {
      emptyPaddedSpacesStrings = new String[250];
      for (int i = 0; i < emptyPaddedSpacesStrings.length; i++) {
        emptyPaddedSpacesStrings[i] = rightPad("", i);
      }
    }
    return emptyPaddedSpacesStrings;
  }

  /**
   * Return the percentage of free memory for this JVM.
   *
   * @return Percentage of free memory.
   */
  public static int getPercentageFreeMemory() {
    Runtime runtime = Runtime.getRuntime();
    long maxMemory = runtime.maxMemory();
    long allocatedMemory = runtime.totalMemory();
    long freeMemory = runtime.freeMemory();
    long totalFreeMemory = (freeMemory + (maxMemory - allocatedMemory));

    return (int) Math.round(100 * (double) totalFreeMemory / maxMemory);
  }

  /**
   * Return non digits only.
   *
   * @return non digits in a string.
   */
  public static String removeDigits(String input) {
    if (Utils.isEmpty(input)) {
      return null;
    }
    StringBuilder digitsOnly = new StringBuilder();
    char c;
    for (int i = 0; i < input.length(); i++) {
      c = input.charAt(i);
      if (!Character.isDigit(c)) {
        digitsOnly.append(c);
      }
    }
    return digitsOnly.toString();
  }

  /**
   * Return digits only.
   *
   * @return digits in a string.
   */
  public static String getDigitsOnly(String input) {
    if (Utils.isEmpty(input)) {
      return null;
    }
    StringBuilder digitsOnly = new StringBuilder();
    char c;
    for (int i = 0; i < input.length(); i++) {
      c = input.charAt(i);
      if (Character.isDigit(c)) {
        digitsOnly.append(c);
      }
    }
    return digitsOnly.toString();
  }

  /**
   * Remove time from a date.
   *
   * @return a date without hour.
   */
  public static Date removeTimeFromDate(Date input) {
    if (input == null) {
      return null;
    }
    // Get an instance of the Calendar.
    Calendar calendar = Calendar.getInstance();

    // Make sure the calendar will not perform automatic correction.
    calendar.setLenient(false);

    // Set the time of the calendar to the given date.
    calendar.setTime(input);

    // Remove the hours, minutes, seconds and milliseconds.
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);

    // Return the date again.
    return calendar.getTime();
  }

  /**
   * Escape HTML content. i.e. replace characters with &amp;values;
   *
   * @param content content
   * @return escaped content
   */
  public static String escapeHtml(String content) {
    if (Utils.isEmpty(content)) {
      return content;
    }
    return StringEscapeUtils.escapeHtml(content);
  }

  /**
   * UnEscape HTML content. i.e. replace characters with &amp;values;
   *
   * @param content content
   * @return unescaped content
   */
  public static String unEscapeHtml(String content) {
    if (Utils.isEmpty(content)) {
      return content;
    }
    return StringEscapeUtils.unescapeHtml(content);
  }

  /**
   * UnEscape XML content. i.e. replace characters with &amp;values;
   *
   * @param content content
   * @return unescaped content
   */
  public static String unEscapeXml(String content) {
    if (Utils.isEmpty(content)) {
      return content;
    }
    return StringEscapeUtils.unescapeXml(content);
  }

  /**
   * Escape SQL content. i.e. replace characters with &amp;values;
   *
   * @param content content
   * @return escaped content
   */
  public static String escapeSql(String content) {
    if (Utils.isEmpty(content)) {
      return content;
    }
    return StringEscapeUtils.escapeSql(content);
  }

  /**
   * Remove CR / LF from String - Better performance version - Doesn't NPE - 40 times faster on an
   * empty string - 2 times faster on a mixed string - 25% faster on 2 char string with only CRLF in
   * it
   *
   * @param in input
   * @return cleaned string
   */
  public static String removeCRLF(String in) {
    if ((in != null) && (in.length() > 0)) {
      int inLen = in.length();
      int posn = 0;
      char[] tmp = new char[inLen];
      char ch;
      for (int i = 0; i < inLen; i++) {
        ch = in.charAt(i);
        if ((ch != '\n' && ch != '\r')) {
          tmp[posn] = ch;
          posn++;
        }
      }
      return new String(tmp, 0, posn);
    } else {
      return "";
    }
  }

  /**
   * Remove Character from String - Better performance version - Doesn't NPE - 40 times faster on an
   * empty string - 2 times faster on a mixed string - 25% faster on 2 char string with only
   * CR/LF/TAB in it
   *
   * @param in input
   * @return cleaned string
   */
  public static String removeChar(String in, char badChar) {
    if ((in != null) && (in.length() > 0)) {
      int inLen = in.length();
      int posn = 0;
      char[] tmp = new char[inLen];
      char ch;
      for (int i = 0; i < inLen; i++) {
        ch = in.charAt(i);
        if (ch != badChar) {
          tmp[posn] = ch;
          posn++;
        }
      }
      return new String(tmp, 0, posn);
    } else {
      return "";
    }
  }

  /**
   * Remove CR / LF from String
   *
   * @param in input
   * @return cleaned string
   */
  public static String removeCR(String in) {
    return removeChar(in, '\r');
  } // removeCR

  /**
   * Remove CR / LF from String
   *
   * @param in input
   * @return cleaned string
   */
  public static String removeLF(String in) {
    return removeChar(in, '\n');
  } // removeCRLF

  /**
   * Remove horizontal tab from string
   *
   * @param in input
   * @return cleaned string
   */
  public static String removeTAB(String in) {
    return removeChar(in, '\t');
  }

  /**
   * Add time to an input date
   *
   * @param input the date
   * @param time the time to add (in string)
   * @param dateFormat the time format
   * @return date = input + time
   */
  public static Date addTimeToDate(Date input, String time, String dateFormat) throws HopException {
    if (Utils.isEmpty(time)) {
      return input;
    }
    if (input == null) {
      return null;
    }
    String dateformatString = NVL(dateFormat, "HH:mm:ss");
    int t = decodeTime(time, dateformatString);
    return new Date(input.getTime() + t);
  }

  // Decodes a time value in specified date format and returns it as milliseconds since midnight.
  public static int decodeTime(String s, String dateFormat) throws HopException {
    SimpleDateFormat f = new SimpleDateFormat(dateFormat);
    TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");
    f.setTimeZone(utcTimeZone);
    f.setLenient(false);
    ParsePosition p = new ParsePosition(0);
    Date d = f.parse(s, p);
    if (d == null) {
      throw new HopException("Invalid time value " + dateFormat + ": \"" + s + "\".");
    }
    return (int) d.getTime();
  }

  /**
   * Get the number of occurrences of searchFor in string.
   *
   * @param string String to be searched
   * @param searchFor to be counted string
   * @return number of occurrences
   */
  public static int getOccurenceString(String string, String searchFor) {
    if (string == null || string.length() == 0) {
      return 0;
    }
    int counter = 0;
    int len = searchFor.length();
    if (len > 0) {
      int start = string.indexOf(searchFor);
      while (start != -1) {
        counter++;
        start = string.indexOf(searchFor, start + len);
      }
    }
    return counter;
  }

  public static String[] getAvailableFontNames() {
    GraphicsEnvironment ge = GraphicsEnvironment.getLocalGraphicsEnvironment();
    Font[] fonts = ge.getAllFonts();
    String[] fontName = new String[fonts.length];
    for (int i = 0; i < fonts.length; i++) {
      fontName[i] = fonts[i].getFontName();
    }
    return fontName;
  }

  /**
   * Mask XML content. i.e. protect with CDATA;
   *
   * @param content content
   * @return protected content
   */
  public static String protectXmlCdata(String content) {
    if (Utils.isEmpty(content)) {
      return content;
    }
    return "<![CDATA[" + content + "]]>";
  }

  /**
   * Get the number of occurrences of searchFor in string.
   *
   * @param string String to be searched
   * @param searchFor to be counted string
   * @return number of occurrences
   */
  public static int getOcuranceString(String string, String searchFor) {
    if (string == null || string.length() == 0) {
      return 0;
    }
    Pattern p = Pattern.compile(searchFor);
    Matcher m = p.matcher(string);
    int count = 0;
    while (m.find()) {
      ++count;
    }
    return count;
  }

  /**
   * Mask XML content. i.e. replace characters with &amp;values;
   *
   * @param content content
   * @return masked content
   */
  public static String escapeXml(String content) {
    if (Utils.isEmpty(content)) {
      return content;
    }
    return StringEscapeUtils.escapeXml(content);
  }

  /**
   * New method avoids string concatenation is between 20% and > 2000% faster depending on length of
   * the string to pad, and the size to pad it to. For larger amounts to pad, (e.g. pad a 4
   * character string out to 20 places) this is orders of magnitude faster.
   *
   * @param valueToPad the string to pad
   * @param filler the pad string to fill with
   * @param size the size to pad to
   * @return the new string, padded to the left
   *     <p>Note - The original method was flawed in a few cases:
   *     <p>1- The filler could be a string of any length - and the returned string was not
   *     necessarily limited to size. So a 3 character pad of an 11 character string could end up
   *     being 17 characters long. 2- For a pad of zero characters ("") the former method would
   *     enter an infinite loop. 3- For a null pad, it would throw an NPE 4- For a null valueToPad,
   *     it would throw an NPE
   */
  public static String lpad(String valueToPad, String filler, int size) {
    if ((size == 0) || (valueToPad == null) || (filler == null)) {
      return valueToPad;
    }
    int vSize = valueToPad.length();
    int fSize = filler.length();
    // This next if ensures previous behavior, but prevents infinite loop
    // if "" is passed in as a filler.
    if ((vSize >= size) || (fSize == 0)) {
      return valueToPad;
    }
    int tgt = (size - vSize);
    StringBuilder sb = new StringBuilder(size);
    sb.append(filler);
    while (sb.length() < tgt) {
      // instead of adding one character at a time, this
      // is exponential - much fewer times in loop
      sb.append(sb);
    }
    sb.append(valueToPad);
    return sb.substring(
        Math.max(
            0, sb.length() - size)); // this makes sure you have the right size string returned.
  }

  /**
   * New method avoids string concatenation is between 50% and > 2000% faster depending on length of
   * the string to pad, and the size to pad it to. For larger amounts to pad, (e.g. pad a 4
   * character string out to 20 places) this is orders of magnitude faster.
   *
   * @param valueToPad the string to pad
   * @param filler the pad string to fill with
   * @param size the size to pad to
   * @return The string, padded to the right
   *     <p>1- The filler can still be a string of any length - and the returned string was not
   *     necessarily limited to size. So a 3 character pad of an 11 character string with a size of
   *     15 could end up being 17 characters long (instead of the "asked for 15"). 2- For a pad of
   *     zero characters ("") the former method would enter an infinite loop. 3- For a null pad, it
   *     would throw an NPE 4- For a null valueToPad, it would throw an NPE
   */
  public static String rpad(String valueToPad, String filler, int size) {
    if ((size == 0) || (valueToPad == null) || (filler == null)) {
      return valueToPad;
    }
    int vSize = valueToPad.length();
    int fSize = filler.length();
    // This next if ensures previous behavior, but prevents infinite loop
    // if "" is passed in as a filler.
    if ((vSize >= size) || (fSize == 0)) {
      return valueToPad;
    }
    int tgt = (size - vSize);
    StringBuilder sb1 = new StringBuilder(size);
    sb1.append(filler);
    while (sb1.length() < tgt) {
      // instead of adding one character at a time, this
      // is exponential - much fewer times in loop
      sb1.append(sb1);
    }
    StringBuilder sb = new StringBuilder(valueToPad);
    sb.append(sb1);
    return sb.substring(0, size);
  }

  public static boolean classIsOrExtends(Class<?> clazz, Class<?> superClass) {
    if (clazz.equals(Object.class)) {
      return false;
    }
    return clazz.equals(superClass) || classIsOrExtends(clazz.getSuperclass(), superClass);
  }

  public static String getDeprecatedPrefix() {
    return " " + BaseMessages.getString(PKG, "Const.Deprecated");
  }
}
