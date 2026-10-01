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

import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.ai.advisor.AiAdvisorPluginType;
import org.apache.hop.ai.advisor.AiAdvisorPrompt;
import org.apache.hop.ai.advisor.IAiAdvisor;
import org.apache.hop.ai.config.HopAiConfig;
import org.apache.hop.ai.config.HopAiConfigSingleton;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Standing notes appended to every advisor system prompt.
 *
 * <p>Sources, in order: plugin-folder / classpath {@code ai-context.md} and {@code
 * ai-context/<advisor-id>.md}, {@link IAiAdvisor#getStandingContext()}, then Configuration extra
 * notes and context files (for example {@code AGENTS.md}). Plugin-folder files overlay classpath
 * files of the same name so an install can edit notes without rebuilding.
 */
public final class AiAdvisorExtraContext {

  public static final String SHARED_FILE = "ai-context.md";
  public static final String ADVISOR_FILE_DIR = "ai-context";

  static final int MAX_NOTES_CHARS = 20_000;
  static final int MAX_FILE_CHARS = 40_000;

  private AiAdvisorExtraContext() {}

  public static void apply(AiAdvisorPrompt prompt, IVariables variables) {
    apply(prompt, null, HopAiConfigSingleton.getConfig(), variables);
  }

  public static void apply(AiAdvisorPrompt prompt, IAiAdvisor advisor, IVariables variables) {
    apply(prompt, advisor, HopAiConfigSingleton.getConfig(), variables);
  }

  static void apply(AiAdvisorPrompt prompt, HopAiConfig config, IVariables variables) {
    apply(prompt, null, config, variables);
  }

  static void apply(
      AiAdvisorPrompt prompt, IAiAdvisor advisor, HopAiConfig config, IVariables variables) {
    if (prompt == null) {
      return;
    }
    String extra = buildSystemAppendix(advisor, config, variables);
    if (Utils.isEmpty(extra)) {
      return;
    }
    String system = prompt.getSystemPrompt();
    prompt.setSystemPrompt(Utils.isEmpty(system) ? extra : system + "\n\n" + extra);
  }

  public static List<String> sharingPhrases(IVariables variables, String notesPhrase) {
    return sharingPhrases(null, HopAiConfigSingleton.getConfig(), variables, notesPhrase);
  }

  public static List<String> sharingPhrases(
      IAiAdvisor advisor, IVariables variables, String notesPhrase) {
    return sharingPhrases(advisor, HopAiConfigSingleton.getConfig(), variables, notesPhrase);
  }

  static List<String> sharingPhrases(HopAiConfig config, IVariables variables, String notesPhrase) {
    return sharingPhrases(null, config, variables, notesPhrase);
  }

  static List<String> sharingPhrases(
      IAiAdvisor advisor, HopAiConfig config, IVariables variables, String notesPhrase) {
    List<String> phrases = new ArrayList<>();
    for (LoadedFile file : loadPluginContext(advisor)) {
      phrases.add(file.label);
    }
    if (advisor != null && !Utils.isEmpty(advisor.getStandingContext())) {
      phrases.add("plugin notes");
    }
    if (config != null) {
      if (!Utils.isEmpty(resolvedNotes(config, variables))) {
        phrases.add(Utils.isEmpty(notesPhrase) ? "extra notes" : notesPhrase);
      }
      for (LoadedFile file : loadFiles(config, variables)) {
        phrases.add(file.label);
      }
    }
    return phrases;
  }

  static String buildSystemAppendix(IAiAdvisor advisor, HopAiConfig config, IVariables variables) {
    StringBuilder extra = new StringBuilder();
    List<LoadedFile> pluginFiles = loadPluginContext(advisor);
    if (!pluginFiles.isEmpty()) {
      extra.append(
          "Standing notes shipped with the advisor plugin. Honour these when they affect names, metadata, and conventions.");
      for (LoadedFile file : pluginFiles) {
        extra.append("\n\nFile ").append(file.label).append(":\n").append(file.text);
      }
    }
    if (advisor != null && !Utils.isEmpty(advisor.getStandingContext())) {
      if (!extra.isEmpty()) {
        extra.append("\n\n");
      }
      extra
          .append("Advisor notes:\n")
          .append(
              AiTextUtil.redactSecrets(
                  AiTextUtil.truncate(advisor.getStandingContext().trim(), MAX_NOTES_CHARS)));
    }
    String user = buildUserAppendix(config, variables);
    if (!Utils.isEmpty(user)) {
      if (!extra.isEmpty()) {
        extra.append("\n\n");
      }
      extra.append(user);
    }
    return extra.toString();
  }

  static String buildSystemAppendix(HopAiConfig config, IVariables variables) {
    return buildSystemAppendix(null, config, variables);
  }

  static String buildUserAppendix(HopAiConfig config, IVariables variables) {
    if (config == null) {
      return "";
    }
    String notes = resolvedNotes(config, variables);
    List<LoadedFile> files = loadFiles(config, variables);
    if (Utils.isEmpty(notes) && files.isEmpty()) {
      return "";
    }
    StringBuilder extra = new StringBuilder();
    extra.append(
        "Project and environment notes from Hop AI Assistant options. Honour these when they affect names, metadata, and conventions.");
    if (!Utils.isEmpty(notes)) {
      extra.append("\n\nExtra notes:\n").append(notes);
    }
    for (LoadedFile file : files) {
      extra.append("\n\nFile ").append(file.label).append(":\n").append(file.text);
    }
    return extra.toString();
  }

  static String resolvedNotes(HopAiConfig config, IVariables variables) {
    String notes = config == null ? null : config.getExtraContext();
    if (Utils.isEmpty(notes)) {
      return "";
    }
    if (variables != null) {
      notes = variables.resolve(notes);
    }
    return AiTextUtil.redactSecrets(AiTextUtil.truncate(notes.trim(), MAX_NOTES_CHARS));
  }

  static List<LoadedFile> loadPluginContext(IAiAdvisor advisor) {
    if (advisor == null) {
      return List.of();
    }
    return loadPluginContext(
        advisor.getClass().getClassLoader(), advisor.getId(), pluginDirectory(advisor.getId()));
  }

  static List<LoadedFile> loadPluginContext(
      ClassLoader classLoader, String advisorId, FileObject pluginDir) {
    List<LoadedFile> loaded = new ArrayList<>();
    LoadedFile shared = loadNamed(pluginDir, SHARED_FILE, classLoader, SHARED_FILE);
    if (shared != null) {
      loaded.add(shared);
    }
    if (!Utils.isEmpty(advisorId)) {
      String relative = ADVISOR_FILE_DIR + "/" + advisorId + ".md";
      LoadedFile specific = loadNamed(pluginDir, relative, classLoader, advisorId + ".md");
      if (specific != null) {
        loaded.add(specific);
      }
    }
    return loaded;
  }

  static LoadedFile loadNamed(
      FileObject pluginDir, String relative, ClassLoader classLoader, String label) {
    LoadedFile fromFolder = readFromFolder(pluginDir, relative, label);
    if (fromFolder != null) {
      return fromFolder;
    }
    return readFromClasspath(classLoader, relative, label);
  }

  static FileObject pluginDirectory(String advisorId) {
    if (Utils.isEmpty(advisorId)) {
      return null;
    }
    try {
      IPlugin plugin =
          PluginRegistry.getInstance().findPluginWithId(AiAdvisorPluginType.class, advisorId);
      if (plugin == null || plugin.getPluginDirectory() == null) {
        return null;
      }
      URL url = plugin.getPluginDirectory();
      return HopVfs.getFileObject(url.toString());
    } catch (Exception e) {
      LogChannel.GENERAL.logDetailed(
          "AI Assistant could not resolve plugin folder for " + advisorId + ": " + e.getMessage());
      return null;
    }
  }

  static LoadedFile readFromFolder(FileObject pluginDir, String relative, String label) {
    if (pluginDir == null || Utils.isEmpty(relative)) {
      return null;
    }
    FileObject file = null;
    try {
      file = pluginDir.resolveFile(relative);
      return readFileObject(file, label);
    } catch (Exception e) {
      LogChannel.GENERAL.logDetailed(
          "AI Assistant skipped plugin context " + relative + ": " + e.getMessage());
      return null;
    } finally {
      closeQuietly(file);
    }
  }

  static LoadedFile readFromClasspath(ClassLoader classLoader, String resource, String label) {
    if (classLoader == null || Utils.isEmpty(resource)) {
      return null;
    }
    try (InputStream in = classLoader.getResourceAsStream(resource)) {
      if (in == null) {
        return null;
      }
      String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      return toLoadedFile(label, text);
    } catch (Exception e) {
      LogChannel.GENERAL.logDetailed(
          "AI Assistant skipped classpath context " + resource + ": " + e.getMessage());
      return null;
    }
  }

  static List<LoadedFile> loadFiles(HopAiConfig config, IVariables variables) {
    List<LoadedFile> loaded = new ArrayList<>();
    if (config == null) {
      return loaded;
    }
    String spec = config.getExtraContextFiles();
    if (spec == null) {
      spec = HopAiConfig.DEFAULT_EXTRA_CONTEXT_FILES;
    }
    if (Utils.isEmpty(spec)) {
      return loaded;
    }
    LinkedHashSet<String> seen = new LinkedHashSet<>();
    for (String line : spec.split("\\R")) {
      String path = line == null ? "" : line.trim();
      if (path.isEmpty() || path.startsWith("#")) {
        continue;
      }
      if (variables != null) {
        path = variables.resolve(path).trim();
      }
      if (path.isEmpty() || path.contains("${") || !seen.add(path)) {
        continue;
      }
      LoadedFile file = readFile(path);
      if (file != null) {
        loaded.add(file);
      }
    }
    return loaded;
  }

  static LoadedFile readFile(String path) {
    FileObject file = null;
    try {
      file = HopVfs.getFileObject(path);
      return readFileObject(file, null);
    } catch (Exception e) {
      LogChannel.GENERAL.logDetailed(
          "AI Assistant skipped context file " + path + ": " + e.getMessage());
      return null;
    } finally {
      closeQuietly(file);
    }
  }

  static LoadedFile readFileObject(FileObject file, String label) {
    if (file == null) {
      return null;
    }
    try {
      if (!file.exists() || !file.isFile()) {
        return null;
      }
      try (InputStream in = HopVfs.getInputStream(file)) {
        String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        String name = label;
        if (Utils.isEmpty(name) && file.getName() != null) {
          name = file.getName().getBaseName();
        }
        return toLoadedFile(Utils.isEmpty(name) ? file.getName().getPath() : name, text);
      }
    } catch (Exception e) {
      LogChannel.GENERAL.logDetailed("AI Assistant skipped context file: " + e.getMessage());
      return null;
    }
  }

  static LoadedFile toLoadedFile(String label, String text) {
    if (Utils.isEmpty(text)) {
      return null;
    }
    return new LoadedFile(
        Utils.isEmpty(label) ? "context" : label,
        AiTextUtil.redactSecrets(AiTextUtil.truncate(text, MAX_FILE_CHARS)));
  }

  static void closeQuietly(FileObject file) {
    if (file == null) {
      return;
    }
    try {
      file.close();
    } catch (Exception ignored) {
      // ignore
    }
  }

  static final class LoadedFile {
    final String label;
    final String text;

    LoadedFile(String label, String text) {
      this.label = label;
      this.text = text;
    }
  }
}
