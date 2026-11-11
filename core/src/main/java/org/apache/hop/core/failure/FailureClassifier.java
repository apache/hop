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

package org.apache.hop.core.failure;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.core.Const;

/**
 * Builds the {@link FailureCause} of a failure and decides its {@link FailureCategory} with fixed
 * rules on exception class names and SQL states. The text of a message only counts in two places:
 * exception class names are read from a logged stack trace, and "No space left on device" in an
 * IOException means RESOURCE.
 *
 * <p>The rules are tried from the root cause outwards: the innermost exception that matches a rule
 * decides. A wrapping HopException rarely says more than the exception it wraps.
 */
public final class FailureClassifier {

  /** Longest message kept in a {@link FailureCause}. */
  public static final int MAX_MESSAGE_LENGTH = 4_000;

  /** Longest stack trace kept in a {@link FailureCause}. */
  public static final int MAX_STACK_TRACE_LENGTH = 16_000;

  private static final int MAX_CHAIN_LENGTH = 50;

  /** A fully qualified exception or error class name, as printed in a stack trace. */
  private static final Pattern EXCEPTION_CLASS_NAME =
      Pattern.compile("\\b((?:[a-z_$][\\w$]*\\.)+[A-Z][\\w$]*(?:Exception|Error))(?=[:\\s]|$)");

  private static final Map<String, FailureCategory> CLASS_RULES =
      Map.ofEntries(
          // Connectivity
          Map.entry("java.net.ConnectException", FailureCategory.CONNECTIVITY),
          Map.entry("java.net.UnknownHostException", FailureCategory.CONNECTIVITY),
          Map.entry("java.net.NoRouteToHostException", FailureCategory.CONNECTIVITY),
          Map.entry("java.net.PortUnreachableException", FailureCategory.CONNECTIVITY),
          Map.entry("java.net.SocketTimeoutException", FailureCategory.CONNECTIVITY),
          Map.entry("java.net.SocketException", FailureCategory.CONNECTIVITY),
          Map.entry("java.net.http.HttpConnectTimeoutException", FailureCategory.CONNECTIVITY),
          Map.entry("java.net.http.HttpTimeoutException", FailureCategory.CONNECTIVITY),
          Map.entry("javax.net.ssl.SSLException", FailureCategory.CONNECTIVITY),
          Map.entry("java.sql.SQLTransientConnectionException", FailureCategory.CONNECTIVITY),
          Map.entry("java.sql.SQLNonTransientConnectionException", FailureCategory.CONNECTIVITY),
          Map.entry("java.sql.SQLTimeoutException", FailureCategory.CONNECTIVITY),
          // Authentication and permissions
          Map.entry(
              "java.sql.SQLInvalidAuthorizationSpecException", FailureCategory.AUTHENTICATION),
          Map.entry("javax.security.auth.login.LoginException", FailureCategory.AUTHENTICATION),
          Map.entry("javax.naming.AuthenticationException", FailureCategory.AUTHENTICATION),
          Map.entry("javax.naming.NoPermissionException", FailureCategory.AUTHENTICATION),
          Map.entry("java.nio.file.AccessDeniedException", FailureCategory.AUTHENTICATION),
          Map.entry("jakarta.mail.AuthenticationFailedException", FailureCategory.AUTHENTICATION),
          // Schema
          Map.entry("java.sql.SQLSyntaxErrorException", FailureCategory.SCHEMA_MISMATCH),
          // Data conversion
          Map.entry("java.lang.NumberFormatException", FailureCategory.DATA_CONVERSION),
          Map.entry("java.lang.ArithmeticException", FailureCategory.DATA_CONVERSION),
          Map.entry("java.time.format.DateTimeParseException", FailureCategory.DATA_CONVERSION),
          Map.entry("java.text.ParseException", FailureCategory.DATA_CONVERSION),
          Map.entry("java.nio.charset.CharacterCodingException", FailureCategory.DATA_CONVERSION),
          Map.entry("java.sql.SQLDataException", FailureCategory.DATA_CONVERSION),
          Map.entry(
              "org.apache.hop.core.exception.HopValueException", FailureCategory.DATA_CONVERSION),
          Map.entry(
              "org.apache.hop.core.exception.HopConversionException",
              FailureCategory.DATA_CONVERSION),
          // Constraints
          Map.entry(
              "java.sql.SQLIntegrityConstraintViolationException",
              FailureCategory.CONSTRAINT_VIOLATION),
          // Missing input
          Map.entry("java.io.FileNotFoundException", FailureCategory.MISSING_INPUT),
          Map.entry("java.nio.file.NoSuchFileException", FailureCategory.MISSING_INPUT),
          Map.entry("org.apache.commons.vfs2.FileNotFoundException", FailureCategory.MISSING_INPUT),
          Map.entry(
              "org.apache.hop.core.exception.HopFileNotFoundException",
              FailureCategory.MISSING_INPUT),
          // Resources
          Map.entry("java.lang.OutOfMemoryError", FailureCategory.RESOURCE),
          Map.entry("java.lang.StackOverflowError", FailureCategory.RESOURCE),
          Map.entry("java.sql.SQLTransactionRollbackException", FailureCategory.RESOURCE),
          // Configuration
          Map.entry("java.lang.ClassNotFoundException", FailureCategory.CONFIGURATION),
          Map.entry("java.lang.NoClassDefFoundError", FailureCategory.CONFIGURATION),
          Map.entry("java.lang.NoSuchMethodError", FailureCategory.CONFIGURATION),
          Map.entry("java.sql.SQLFeatureNotSupportedException", FailureCategory.CONFIGURATION),
          Map.entry(
              "org.apache.hop.core.exception.HopMissingPluginsException",
              FailureCategory.CONFIGURATION),
          Map.entry(
              "org.apache.hop.core.exception.HopPluginNotFoundException",
              FailureCategory.CONFIGURATION),
          Map.entry(
              "org.apache.hop.core.exception.HopConfigException", FailureCategory.CONFIGURATION));

  private FailureClassifier() {
    // Static helpers only
  }

  /** The cause of a failure from the exception that was thrown. */
  public static FailureCause causeOf(Throwable throwable) {
    if (throwable == null) {
      return null;
    }
    List<Throwable> chain = chainOf(throwable);
    Throwable root = chain.get(chain.size() - 1);

    FailureCause cause = new FailureCause();
    cause.setExceptionClass(throwable.getClass().getName());
    cause.setMessage(cut(throwable.getMessage(), MAX_MESSAGE_LENGTH));
    cause.setRootCauseClass(root.getClass().getName());
    cause.setRootCauseMessage(cut(root.getMessage(), MAX_MESSAGE_LENGTH));
    cause.setStackTrace(cut(Const.getStackTracker(throwable), MAX_STACK_TRACE_LENGTH));
    for (Throwable t : chain) {
      if (t instanceof SQLException sqlException && sqlException.getSQLState() != null) {
        cause.setSqlState(sqlException.getSQLState());
        cause.setVendorCode(sqlException.getErrorCode());
        break;
      }
    }
    return cause;
  }

  /**
   * The cause of a failure from an error that was logged, not thrown: the logged message and, if
   * one was logged with it, the stack trace. Exception class names are read from that text.
   */
  public static FailureCause causeFromLog(String message, String stackTrace) {
    if (message == null && stackTrace == null) {
      return null;
    }
    List<String> classNames = exceptionClassNames(join(message, stackTrace));

    FailureCause cause = new FailureCause();
    cause.setFromLog(true);
    cause.setMessage(cut(message, MAX_MESSAGE_LENGTH));
    cause.setStackTrace(cut(stackTrace, MAX_STACK_TRACE_LENGTH));
    if (!classNames.isEmpty()) {
      cause.setExceptionClass(classNames.get(0));
      cause.setRootCauseClass(classNames.get(classNames.size() - 1));
    }
    return cause;
  }

  /** The category of a thrown exception. */
  public static FailureCategory classify(Throwable throwable) {
    if (throwable == null) {
      return FailureCategory.UNKNOWN;
    }
    List<Throwable> chain = chainOf(throwable);
    for (int i = chain.size() - 1; i >= 0; i--) {
      FailureCategory category = categoryOf(chain.get(i));
      if (category != null) {
        return category;
      }
    }
    return FailureCategory.UNKNOWN;
  }

  /** The category of a logged error, from the exception class names in its text. */
  public static FailureCategory classifyText(String message, String stackTrace) {
    String text = join(message, stackTrace);
    List<String> classNames = exceptionClassNames(text);
    for (int i = classNames.size() - 1; i >= 0; i--) {
      FailureCategory category = CLASS_RULES.get(classNames.get(i));
      if (category != null) {
        return category;
      }
    }
    if (isOutOfDiskSpace(text)) {
      return FailureCategory.RESOURCE;
    }
    return FailureCategory.UNKNOWN;
  }

  /** The category the SQL state of a database error stands for, or null if it decides nothing. */
  static FailureCategory categoryOfSqlState(String sqlState) {
    if (sqlState == null || sqlState.length() < 2) {
      return null;
    }
    switch (sqlState) {
      case "42501":
        return FailureCategory.AUTHENTICATION;
      case "42601":
        return FailureCategory.CONFIGURATION;
      case "42000":
        // MySQL uses it for syntax errors and access denied alike
        return null;
      case "57P01", "57P02", "57P03", "HYT00", "HYT01":
        return FailureCategory.CONNECTIVITY;
      default:
        break;
    }
    switch (sqlState.substring(0, 2)) {
      case "08":
        return FailureCategory.CONNECTIVITY;
      case "28":
        return FailureCategory.AUTHENTICATION;
      case "42", "3F":
        return FailureCategory.SCHEMA_MISMATCH;
      case "22":
        return FailureCategory.DATA_CONVERSION;
      case "23":
        return FailureCategory.CONSTRAINT_VIOLATION;
      case "40", "53", "54":
        return FailureCategory.RESOURCE;
      case "3D":
        return FailureCategory.CONFIGURATION;
      default:
        return null;
    }
  }

  private static FailureCategory categoryOf(Throwable throwable) {
    if (throwable instanceof SQLException sqlException) {
      FailureCategory category = categoryOfSqlState(sqlException.getSQLState());
      if (category != null) {
        return category;
      }
    }
    // The class or one of its superclasses, so that SSLHandshakeException counts as SSLException
    for (Class<?> c = throwable.getClass();
        c != null && c != Throwable.class;
        c = c.getSuperclass()) {
      FailureCategory category = CLASS_RULES.get(c.getName());
      if (category != null) {
        return category;
      }
    }
    if (throwable instanceof IOException && isOutOfDiskSpace(throwable.getMessage())) {
      return FailureCategory.RESOURCE;
    }
    return null;
  }

  private static boolean isOutOfDiskSpace(String text) {
    return text != null && text.contains("No space left on device");
  }

  /** The exception and its causes, outermost first, without loops. */
  private static List<Throwable> chainOf(Throwable throwable) {
    List<Throwable> chain = new ArrayList<>();
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable t = throwable;
        t != null && chain.size() < MAX_CHAIN_LENGTH && seen.add(t);
        t = t.getCause()) {
      chain.add(t);
    }
    return chain;
  }

  private static List<String> exceptionClassNames(String text) {
    List<String> names = new ArrayList<>();
    if (text == null) {
      return names;
    }
    for (String line : text.split("\\R")) {
      // Skip stack frames: "at org.example.Foo.bar(Foo.java:12)"
      if (line.trim().startsWith("at ")) {
        continue;
      }
      Matcher matcher = EXCEPTION_CLASS_NAME.matcher(line);
      while (matcher.find()) {
        names.add(matcher.group(1));
      }
    }
    return names;
  }

  private static String join(String message, String stackTrace) {
    if (message == null) {
      return stackTrace;
    }
    if (stackTrace == null) {
      return message;
    }
    return message + "\n" + stackTrace;
  }

  private static String cut(String text, int maxLength) {
    if (text == null || text.length() <= maxLength) {
      return text;
    }
    return text.substring(0, maxLength) + "...";
  }
}
