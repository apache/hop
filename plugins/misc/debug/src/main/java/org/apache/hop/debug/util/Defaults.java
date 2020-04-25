package org.apache.hop.debug.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Defaults {
  public static final String DEBUG_GROUP = "debug_plugin";

  public static final String TRANSFORM_ATTR_LOGLEVEL = "log_level";
  public static final String TRANSFORM_ATTR_START_ROW = "start_row";
  public static final String TRANSFORM_ATTR_END_ROW = "end_row";
  public static final String TRANSFORM_ATTR_CONDITION = "condition";

  public static final String ACTION_ATTR_LOGLEVEL = "action_log_level";
  public static final String ACTION_ATTR_LOG_RESULT = "action_log_result";
  public static final String ACTION_ATTR_LOG_VARIABLES = "action_log_variables";
  public static final String ACTION_ATTR_LOG_RESULT_ROWS = "action_log_result_rows";
  public static final String ACTION_ATTR_LOG_RESULT_FILES = "action_log_result_files";

  public static final String VARIABLE_HOP_DEBUG_DURATION = "HOP_DEBUG_DURATION";

  public static final Set<String> VARIABLES_TO_IGNORE = getVariablesToIgnore();

  private static Set<String> getVariablesToIgnore() {
    Set<String> strings = new HashSet<>();

    List<String> valuesList = Arrays.asList();
    strings.addAll( valuesList );

    return strings;
  }
}
