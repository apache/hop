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

package org.apache.hop.ui.hopgui;

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;

/** Parses Hop GUI command line options in both picocli and {@code -name=value} forms. */
public final class HopGuiCommandLine {

  public static final String[] PROJECT_OPTION_NAMES = {"-j", "--project", "-project"};
  public static final String[] ENVIRONMENT_OPTION_NAMES = {"-e", "--environment", "-environment"};
  public static final String[] FILE_OPTION_NAMES = {"-f", "--file", "-file"};

  private HopGuiCommandLine() {
    // utility
  }

  /**
   * Find an option in the argument list. Accepts {@code -name value}, {@code --name value}, and
   * {@code -name=value} / {@code --name=value}.
   *
   * @param args argument list (may be null)
   * @param names option names including dashes, e.g. {@code -j}, {@code --project}
   * @return the option value, or null when not present
   */
  public static String findOption(List<String> args, String... names) {
    if (args == null || names == null) {
      return null;
    }
    for (int i = 0; i < args.size(); i++) {
      String arg = args.get(i);
      if (arg == null) {
        continue;
      }
      for (String name : names) {
        if (StringUtils.isEmpty(name)) {
          continue;
        }
        if (arg.equals(name)) {
          if (i + 1 < args.size() && !isFlag(args.get(i + 1))) {
            return args.get(i + 1);
          }
        } else if (arg.startsWith(name + "=")) {
          String value = arg.substring(name.length() + 1).trim();
          if (StringUtils.isNotEmpty(value)) {
            return value;
          }
        }
      }
    }
    return null;
  }

  /**
   * Find a file option and remove it from the list so later URL/CLI handling does not reopen it.
   *
   * @param args argument list (modified in place)
   * @param names option names including dashes
   * @return the option value, or null when not present
   */
  public static String takeOption(List<String> args, String... names) {
    if (args == null || names == null) {
      return null;
    }
    for (int i = 0; i < args.size(); i++) {
      String arg = args.get(i);
      if (arg == null) {
        continue;
      }
      for (String name : names) {
        if (StringUtils.isEmpty(name)) {
          continue;
        }
        if (arg.equals(name)) {
          args.remove(i);
          if (i < args.size() && !isFlag(args.get(i))) {
            return args.remove(i);
          }
          return null;
        }
        if (arg.startsWith(name + "=")) {
          args.remove(i);
          String value = arg.substring(name.length() + 1).trim();
          return StringUtils.isNotEmpty(value) ? value : null;
        }
      }
    }
    return null;
  }

  /**
   * Resolve a filename, trying the value as-is and then relative to {@code PROJECT_HOME}.
   *
   * @param variables variable space (may be null)
   * @param filePath filename or path
   * @return a path that exists when possible, otherwise the resolved original path
   */
  public static String resolveFile(IVariables variables, String filePath) {
    if (StringUtils.isEmpty(filePath)) {
      return filePath;
    }
    String resolved = variables != null ? variables.resolve(filePath) : filePath;
    if (exists(resolved, variables)) {
      return resolved;
    }
    String projectHome = variables != null ? variables.getVariable("PROJECT_HOME") : null;
    if (StringUtils.isNotEmpty(projectHome)) {
      String alternative =
          variables.resolve(projectHome + "/" + filePath.replaceFirst("^\\./", ""));
      if (exists(alternative, variables)) {
        return alternative;
      }
    }
    return resolved;
  }

  private static boolean exists(String path, IVariables variables) {
    if (StringUtils.isEmpty(path)) {
      return false;
    }
    try {
      FileObject fileObject = HopVfs.getFileObject(path, variables);
      return fileObject.exists();
    } catch (Exception e) {
      return false;
    }
  }

  private static boolean isFlag(String arg) {
    return arg != null && arg.startsWith("-");
  }
}
