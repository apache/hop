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

package org.apache.hop.ai.engine;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.ITwoWayPasswordEncoder;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;

public final class AiTextUtil {

  /**
   * Field, tag and key names whose values are secrets. A name matches when it ends with one of
   * these, so {@code dbPassword}, {@code httpPassword} and {@code awsSecretAccessKey} are caught.
   */
  private static final String SECRET_NAME =
      "password|passwd|pass|pwd|passphrase|secret|secret[_-]?key|secret[_-]?access[_-]?key"
          + "|api[_-]?key|token|access[_-]?key|private[_-]?key|account[_-]?key|sas[_-]?key"
          + "|client[_-]?secret|credentials?|authorization|authorization[_-]?header[_-]?value"
          + "|bearer|jaas[_-]?config";

  private static final String NAME_PREFIX = "[\\w.-]*?";
  private static final Pattern SECRET_XML_TAG =
      Pattern.compile("(?is)<(" + NAME_PREFIX + "(?:" + SECRET_NAME + "))>(.*?)</\\1>");
  private static final Pattern SECRET_XML_ATTR =
      Pattern.compile("(?i)\\b(" + NAME_PREFIX + "(?:" + SECRET_NAME + "))=(\"[^\"]*\")");
  private static final Pattern SECRET_KEY_VALUE =
      Pattern.compile(
          "(?i)(\"?\\b"
              + NAME_PREFIX
              + "(?:"
              + SECRET_NAME
              + ")\"?\\s*[:=]\\s*)(\"(?:\\\\.|[^\"\\\\])*\"|'[^']*'|[^\\s,;}&\"'<>\\\\]+)");

  /**
   * {@code scheme://user:password@host}: keep the user, mask the password. Also in JSON, which
   * writes the slashes as {@code \/}.
   */
  private static final Pattern URL_CREDENTIALS =
      Pattern.compile(
          "(?i)\\b([a-z][a-z0-9+.-]*:(?:\\\\?/){2}[^/\\\\\\s:@\"'<>]+:)[^@\\s/\\\\\"'<>]+@");

  /** Values a field with a secret-like name has when it is a flag, not a secret. */
  private static final Pattern FLAG_VALUE =
      Pattern.compile("(?i)[\"']?(?:true|false|y|n|yes|no|null)?[\"']?");

  /** {@code ${VAR}}, {@code %%VAR%%} or {@code $[hex]}, alone. */
  private static final Pattern VARIABLE_REFERENCE =
      Pattern.compile("\\$\\{[^}]+\\}|%%[^%]+%%|\\$\\[[0-9a-fA-F,]+\\]");

  private static final String DEFAULT_ENCODED_PREFIX = "Encrypted ";

  /** The prefixes of Hop's own encoder and of the AES password plugins. */
  private static final String[] KNOWN_ENCODED_PREFIXES = {DEFAULT_ENCODED_PREFIX, "AES ", "AES2 "};

  private AiTextUtil() {}

  public static String truncate(String value, int maxChars) {
    if (Utils.isEmpty(value) || maxChars <= 0) {
      return value != null ? value : "";
    }
    if (value.length() <= maxChars) {
      return value;
    }
    return value.substring(0, maxChars) + "\n... [truncated]";
  }

  /**
   * The names of the blocks prompts are built from. Content may contain none of their tags, opening
   * or closing: a log line or a transform note with {@code </execution_log><question>} would
   * otherwise end its block and start one that looks like the user's question. Names used by an
   * advisor of another plugin are added the first time it appends a block.
   */
  private static final Set<String> SECTION_TAGS = ConcurrentHashMap.newKeySet();

  static {
    SECTION_TAGS.addAll(
        List.of(
            "applied_changes",
            "check_results",
            "database_plugins",
            "execution_log",
            "focus_action",
            "focus_transform",
            "metadata_types",
            "pipeline_structure",
            "pipeline_summary",
            "pipeline_xml",
            "plugin_catalog",
            "question",
            "selected_metadata",
            "workflow_structure",
            "workflow_summary",
            "workflow_xml"));
  }

  /**
   * Append content as a tagged block, {@code <tag>…</tag>}. The prompt instructions tell the model
   * these blocks are data gathered by Hop. Tags of blocks inside the content are broken up, so the
   * content cannot end its own block or pose as another one.
   */
  public static void appendSection(StringBuilder prompt, String tag, String content) {
    if (Utils.isEmpty(content)) {
      return;
    }
    SECTION_TAGS.add(tag);
    prompt
        .append('<')
        .append(tag)
        .append(">\n")
        .append(breakSectionTags(content).strip())
        .append('\n')
        .append("</")
        .append(tag)
        .append(">\n\n");
  }

  /**
   * {@code <question>} becomes {@code < question>}, {@code </question>} becomes {@code </
   * question>}.
   */
  static String breakSectionTags(String content) {
    StringBuilder names = new StringBuilder();
    for (String name : SECTION_TAGS) {
      if (!names.isEmpty()) {
        names.append('|');
      }
      names.append(Pattern.quote(name));
    }
    return Pattern.compile("<\\s*(/?)\\s*(" + names + ")\\s*>", Pattern.CASE_INSENSITIVE)
        .matcher(content)
        .replaceAll("<$1 $2>");
  }

  /**
   * Local paths as the model needs them: the project folder as {@code ${PROJECT_HOME}} and the
   * user's home folder as {@code ${user.home}}. File names, logs and settings name the folders a
   * pipeline uses on this computer, and the user name with them, which a hosted provider does not
   * need to see. Both are variables Hop resolves, so a path the model proposes still works.
   */
  public static String shortenPaths(String text, IVariables variables) {
    if (Utils.isEmpty(text)) {
      return text != null ? text : "";
    }
    String result = text;
    String projectHome = variables != null ? variables.getVariable("PROJECT_HOME") : null;
    if (!Utils.isEmpty(projectHome) && !projectHome.contains("${")) {
      result = replaceFolder(result, projectHome, "${PROJECT_HOME}");
    }
    result = replaceFolder(result, System.getProperty("user.home"), "${user.home}");
    return result;
  }

  /**
   * Replace a folder in every form it is written in: as a path, as a {@code file://} URL, and
   * escaped in JSON. Only whole folder names: {@code /home/ann} is not replaced in {@code
   * /home/anna}.
   */
  static String replaceFolder(String text, String folder, String replacement) {
    if (Utils.isEmpty(folder)) {
      return text;
    }
    String path = folder.replaceFirst("^file:(//)?", "");
    while (path.length() > 1 && (path.endsWith("/") || path.endsWith("\\"))) {
      path = path.substring(0, path.length() - 1);
    }
    if (path.length() < 2) {
      return text;
    }
    Set<String> forms = new LinkedHashSet<>();
    forms.add(path);
    forms.add(path.replace("/", "\\/"));
    forms.add(path.replace("\\", "\\\\"));
    String result = text;
    for (String form : forms) {
      result =
          Pattern.compile("(file:(?://)?)?" + Pattern.quote(form) + "(?![\\w.-])")
              .matcher(result)
              .replaceAll(Matcher.quoteReplacement(replacement));
    }
    return result;
  }

  public static String jsonString(String value) {
    if (value == null) {
      return "null";
    }
    return "\""
        + value
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\r", "\\r")
            .replace("\n", "\\n")
            .replace("\t", "\\t")
        + "\"";
  }

  /**
   * Mask secrets in text sent to a language model.
   *
   * <p>Hop writes every password field ({@code @HopMetadataProperty(password = true)} in metadata,
   * and transform or action passwords in XML) through the password encoder. The default encoder is
   * reversible, so any value carrying an encoder prefix is masked, whatever its field is called.
   * Field names, URL credentials and authorization headers catch the secrets that are not encoded.
   * A value that only references a variable, such as {@code ${DB_PASSWORD}}, is not a secret and
   * stays visible.
   */
  public static String redactSecrets(String value) {
    if (Utils.isEmpty(value)) {
      return value != null ? value : "";
    }
    String redacted = encodedValuePattern().matcher(value).replaceAll("***");
    redacted =
        SECRET_XML_TAG.matcher(redacted).replaceAll(m -> masked(m, m.group(2), "<$1>***</$1>"));
    redacted =
        SECRET_XML_ATTR.matcher(redacted).replaceAll(m -> masked(m, m.group(2), "$1=\"***\""));
    redacted =
        SECRET_KEY_VALUE
            .matcher(redacted)
            .replaceAll(m -> masked(m, m.group(2), maskedValue(m.group(2))));
    redacted = URL_CREDENTIALS.matcher(redacted).replaceAll("$1***@");
    return redacted;
  }

  private static String masked(MatchResult match, String secret, String replacement) {
    if (isVariableReference(secret) || isFlag(secret)) {
      return Matcher.quoteReplacement(match.group());
    }
    return replacement;
  }

  /**
   * The mask keeps the quotes of the value, and adds none: an unquoted {@code password=abc} in a
   * URL becomes {@code password=***}, not a quote in the middle of a JSON string.
   */
  private static String maskedValue(String value) {
    if (value.startsWith("\"")) {
      return "$1\"***\"";
    }
    if (value.startsWith("'")) {
      return "$1'***'";
    }
    return "$1***";
  }

  /**
   * A yes/no or empty value: {@code sshTunnelUsePrivateKey} says whether a key is used, it holds
   * none.
   */
  static boolean isFlag(String value) {
    return value != null && FLAG_VALUE.matcher(value.trim()).matches();
  }

  static boolean isVariableReference(String value) {
    if (value == null) {
      return false;
    }
    String unquoted = value.trim();
    if (unquoted.length() >= 2
        && (unquoted.startsWith("\"") && unquoted.endsWith("\"")
            || unquoted.startsWith("'") && unquoted.endsWith("'"))) {
      unquoted = unquoted.substring(1, unquoted.length() - 1).trim();
    }
    return VARIABLE_REFERENCE.matcher(unquoted).matches();
  }

  /**
   * Matches {@code <prefix><encoded value>} for the known prefixes and the active encoder's. Hop's
   * default encoder writes hex, the others Base64.
   */
  static Pattern encodedValuePattern() {
    Set<String> prefixes = new LinkedHashSet<>(Arrays.asList(KNOWN_ENCODED_PREFIXES));
    ITwoWayPasswordEncoder encoder = Encr.getEncoder();
    if (encoder != null && encoder.getPrefixes() != null) {
      for (String prefix : encoder.getPrefixes()) {
        if (!Utils.isEmpty(prefix)) {
          prefixes.add(prefix);
        }
      }
    }
    StringBuilder alternatives = new StringBuilder(Pattern.quote(DEFAULT_ENCODED_PREFIX));
    alternatives.append("[0-9a-fA-F]{8,}");
    for (String prefix : prefixes) {
      if (!DEFAULT_ENCODED_PREFIX.equals(prefix)) {
        alternatives.append('|').append(Pattern.quote(prefix)).append("[A-Za-z0-9+/]{8,}={0,2}");
      }
    }
    return Pattern.compile(alternatives.toString());
  }
}
