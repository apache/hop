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

package org.apache.hop.config.doc;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.FieldInfo;
import org.jboss.jandex.Index;
import org.jboss.jandex.IndexReader;

/**
 * Generates one AsciiDoc partial per configuration plugin from the annotations the configuration
 * perspective already builds its widgets from. Build time only: it reads the Jandex indexes the
 * build writes into every module and the message bundles next to the classes, and instantiates only
 * the plain configuration objects, never Hop itself.
 */
public class ConfigDocGenerator {

  static final DotName CONFIG_PLUGIN =
      DotName.createSimple("org.apache.hop.core.config.plugin.ConfigPlugin");
  static final DotName GUI_WIDGET =
      DotName.createSimple("org.apache.hop.core.gui.plugin.GuiWidgetElement");
  static final DotName GUI_PLUGIN =
      DotName.createSimple("org.apache.hop.core.gui.plugin.GuiPlugin");
  static final String PARENT_ID = "EnterOptionsDialog-GuiWidgetsParent";
  static final String JANDEX_INDEX = "META-INF/jandex.idx";
  static final String AGGREGATE = "all-plugins.adoc";

  /** First line of every page this generator owns, and how it tells them from hand-written ones. */
  static final String GENERATED_MARKER =
      "// Generated from the @ConfigPlugin and @GuiWidgetElement annotations.";

  static final String LICENSE =
      """
      ////
      Licensed to the Apache Software Foundation (ASF) under one
      or more contributor license agreements.  See the NOTICE file
      distributed with this work for additional information
      regarding copyright ownership.  The ASF licenses this file
      to you under the Apache License, Version 2.0 (the
      "License"); you may not use this file except in compliance
      with the License.  You may obtain a copy of the License at
        http://www.apache.org/licenses/LICENSE-2.0
      Unless required by applicable law or agreed to in writing,
      software distributed under the License is distributed on an
      "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
      KIND, either express or implied.  See the License for the
      specific language governing permissions and limitations
      under the License.
      ////
      """;

  record Option(
      String order, String label, String toolTip, String type, boolean variables, String dflt) {}

  record Plugin(
      String id, String description, String name, String configKey, List<Option> options) {}

  public static void main(String[] args) throws Exception {
    Path root = Paths.get(args[0]).toAbsolutePath().normalize();
    Path outDir = Paths.get(args[1]).toAbsolutePath().normalize();

    // Scan this program's own classpath rather than the source tree. Every module declaring a
    // @ConfigPlugin is a declared dependency, so each one is here whether it was just built in the
    // reactor or resolved as a jar from the local repository. Walking target/ directories instead
    // would find nothing for the modules an incremental build left alone.
    List<Source> sources = new ArrayList<>();
    for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
      Source source = Source.of(Paths.get(entry));
      if (source != null) sources.add(source);
    }

    List<Plugin> plugins = new ArrayList<>();
    for (Source source : sources) {
      Index index;
      try (InputStream in = source.open(JANDEX_INDEX)) {
        if (in == null) continue;
        index = new IndexReader(in).read();
      } catch (Exception e) {
        continue;
      }
      for (AnnotationInstance ai : index.getAnnotations(CONFIG_PLUGIN)) {
        if (ai.target().kind() != AnnotationTarget.Kind.CLASS) continue;
        Plugin p = readPlugin(ai.target().asClass(), ai, source);
        if (p != null) plugins.add(p);
      }
    }
    plugins.sort(Comparator.comparing(Plugin::name, String.CASE_INSENSITIVE_ORDER));

    // Only the pages this generator wrote are its business. Anything else in the directory was
    // put there by hand and is left alone, neither counted nor deleted.
    List<Path> generatedBefore = generatedPages(outDir);

    // Order matters here. An empty classpath means every plugin looks like one this generator
    // cannot see, and the report below would then name all of them and ask for fourteen new
    // dependencies - true in its way, and useless. "There is nothing here at all" is the more
    // fundamental thing to have gone wrong, so it is the one to say.
    if (plugins.isEmpty() && !generatedBefore.isEmpty()) {
      throw new IllegalStateException(
          "No configuration plugins found on the classpath, but "
              + outDir.getFileName()
              + " documents "
              + generatedBefore.size()
              + ". Refusing to replace those pages with nothing. This means the classpath is"
              + " missing the modules that declare a @ConfigPlugin - build the project first.");
    }

    reportPluginsOutsideTheClasspath(root, plugins);

    Files.createDirectories(outDir);
    Set<Path> written = new HashSet<>();
    for (Plugin p : plugins) {
      Path file = outDir.resolve(p.id() + ".adoc");
      Files.writeString(file, render(p), StandardCharsets.UTF_8);
      written.add(file);
      System.out.printf(
          "%-34s %2d options -> %s%n", p.name(), p.options().size(), root.relativize(file));
    }

    // One page wants the lot: a section per plugin, in the order the perspective's tree shows them.
    StringBuilder all = new StringBuilder(LICENSE);
    all.append(GENERATED_MARKER).append("\n// Edit those, not this file.\n");
    for (Plugin p : plugins) {
      all.append("\n== ").append(p.name()).append("\n\n").append(body(p));
    }
    Path index = outDir.resolve(AGGREGATE);
    Files.writeString(index, all.toString(), StandardCharsets.UTF_8);
    written.add(index);
    System.out.println("aggregate -> " + root.relativize(index));

    // A plugin that was renamed or removed leaves its page behind, and the pages that include one
    // by name would go on rendering it for good. Nothing reads a generated page that this run did
    // not write, so it goes.
    for (Path stale : generatedBefore) {
      if (!written.contains(stale)) {
        Files.delete(stale);
        System.out.println("removed (no longer generated) -> " + root.relativize(stale));
      }
    }
    System.out.println(
        "\n"
            + plugins.size()
            + " configuration plugins, "
            + plugins.stream().mapToInt(p -> p.options().size()).sum()
            + " options");
  }

  /**
   * Which modules a documented plugin can come from is decided by this module's dependencies, and
   * that list is written by hand. A plugin added to a module that is not on it would be absent from
   * the manual with nothing to say so: the page set would not change, so the drift check in CI
   * would pass, and the option would simply never be written about.
   *
   * <p>So: sweep what the build has actually compiled and see whether anything turns up that the
   * classpath did not. This is deliberately only a report - it never adds to the pages, because the
   * set of modules on disk depends on what this particular build chose to compile, and the pages
   * have to come out the same everywhere. An incremental build that has not compiled the new module
   * simply finds nothing, which is why this is best-effort. A full build - every push to main is
   * one - does find it.
   */
  static void reportPluginsOutsideTheClasspath(Path root, List<Plugin> documented)
      throws IOException {
    Set<String> known = new HashSet<>();
    for (Plugin p : documented) {
      known.add(p.id());
    }

    Map<String, String> missing = new TreeMap<>();
    try (Stream<Path> walk = Files.walk(root)) {
      for (Path index : walk.filter(ConfigDocGenerator::isModuleIndex).toList()) {
        Path classes = index.getParent().getParent();
        Source source = Source.of(classes);
        if (source == null) continue;
        Index jandex;
        try (InputStream in = source.open(JANDEX_INDEX)) {
          if (in == null) continue;
          jandex = new IndexReader(in).read();
        } catch (Exception e) {
          continue;
        }
        for (AnnotationInstance ai : jandex.getAnnotations(CONFIG_PLUGIN)) {
          if (ai.target().kind() != AnnotationTarget.Kind.CLASS) continue;
          String id = str(ai, "id", "");
          if (known.contains(id) || !hasPerspectiveWidgets(ai.target().asClass())) continue;
          // target/classes/META-INF/jandex.idx -> the module directory
          missing.put(id, root.relativize(classes.getParent().getParent()).toString());
        }
      }
    }
    if (missing.isEmpty()) return;

    StringBuilder message =
        new StringBuilder(
            "These configuration plugins add options to the configuration perspective but are in"
                + " modules this generator cannot see, so the manual would not describe them:\n");
    missing.forEach(
        (id, module) ->
            message.append("  ").append(id).append("  in  ").append(module).append("\n"));
    message.append(
        "Add the module as a dependency of config-doc/pom.xml, next to the others, and they will"
            + " be documented on the next build.");
    throw new IllegalStateException(message.toString());
  }

  /** target/classes/META-INF/jandex.idx under some module of this tree. */
  static boolean isModuleIndex(Path path) {
    return path.endsWith(Paths.get("target", "classes", "META-INF", "jandex.idx"));
  }

  /** Whether the class puts any widget on the configuration perspective. */
  static boolean hasPerspectiveWidgets(ClassInfo ci) {
    for (FieldInfo f : ci.fields()) {
      AnnotationInstance w = f.annotation(GUI_WIDGET);
      if (w != null && PARENT_ID.equals(str(w, "parentId", ""))) return true;
    }
    return false;
  }

  /**
   * The .adoc files in the output directory that carry {@link #GENERATED_MARKER}. A file without it
   * was written by hand - a README, a page someone dropped in - and this generator neither counts
   * it nor deletes it.
   */
  static List<Path> generatedPages(Path outDir) throws IOException {
    if (!Files.isDirectory(outDir)) return List.of();
    List<Path> pages = new ArrayList<>();
    try (Stream<Path> files = Files.list(outDir)) {
      for (Path file : files.toList()) {
        if (!file.getFileName().toString().endsWith(".adoc")) continue;
        try {
          if (Files.readString(file, StandardCharsets.UTF_8).contains(GENERATED_MARKER)) {
            pages.add(file);
          }
        } catch (IOException e) {
          // Unreadable, so not something to count on or delete.
        }
      }
    }
    return pages;
  }

  static Plugin readPlugin(ClassInfo ci, AnnotationInstance ai, Source source) {
    List<FieldInfo> fields = new ArrayList<>();
    for (FieldInfo f : ci.fields()) {
      AnnotationInstance w = f.annotation(GUI_WIDGET);
      // No default: parentId() itself defaults to "", so falling back to the config parent
      // here would adopt every widget that simply did not name one.
      if (w != null && PARENT_ID.equals(str(w, "parentId", ""))) fields.add(f);
    }
    if (fields.isEmpty()) return null;

    Properties msgs = messages(source, ci.name().packagePrefix());
    Map<String, Object> defaults = configDefaults(ai);

    List<Option> options = new ArrayList<>();
    for (FieldInfo f : fields) {
      AnnotationInstance w = f.annotation(GUI_WIDGET);
      // The configuration object is the truth where it has this setting; the annotation is the
      // fallback for plugins that keep no such object.
      Object fromConfig = defaults.get(f.name());
      String dflt = fromConfig != null ? String.valueOf(fromConfig) : str(w, "defaultValue", "");
      options.add(
          new Option(
              str(w, "order", "") + str(w, "id", ""),
              translate(str(w, "label", ""), msgs),
              translate(str(w, "toolTip", ""), msgs),
              w.value("type") == null ? "" : w.value("type").asEnum(),
              w.value("variables") == null || w.value("variables").asBoolean(),
              dflt));
    }
    options.sort(Comparator.comparing(Option::order));
    // The configuration perspective labels the tree with the @GuiPlugin description, so that is
    // the name a reader is looking for on the page.
    AnnotationInstance gui = ci.declaredAnnotation(GUI_PLUGIN);
    String name = gui == null ? "" : translate(str(gui, "description", ""), msgs);
    if (name.isEmpty()) {
      name = translate(str(ai, "description", ""), msgs);
    }
    return new Plugin(
        str(ai, "id", ""),
        translate(str(ai, "description", ""), msgs),
        name,
        str(ai, "configKey", ""),
        options);
  }

  /** Instantiates the plugin's configClass and reads its fields: the real, current defaults. */
  static Map<String, Object> configDefaults(AnnotationInstance ai) {
    AnnotationValue cc = ai.value("configClass");
    if (cc == null) return Map.of();
    String name = cc.asClass().name().toString();
    if (name.equals("java.lang.Void")) return Map.of();
    try {
      Class<?> c = Class.forName(name);
      Constructor<?> ctor = c.getDeclaredConstructor();
      ctor.setAccessible(true);
      Object o = ctor.newInstance();
      Map<String, Object> values = new HashMap<>();
      for (Field f : c.getDeclaredFields()) {
        if (Modifier.isStatic(f.getModifiers())) continue;
        f.setAccessible(true);
        Object v = f.get(o);
        if (v != null
            && !(v instanceof Collection<?> c2 && c2.isEmpty())
            && !(v instanceof Map<?, ?> m && m.isEmpty())) {
          values.put(f.getName(), v);
        }
      }
      return values;
    } catch (Throwable t) {
      System.err.println("  ! could not read defaults from " + name + " : " + t);
      return Map.of();
    }
  }

  /** BaseMessages.getString(Class, key) resolves to <package>.messages.messages. */
  static Properties messages(Source source, String pkg) {
    Properties props = new Properties();
    if (pkg == null) return props;
    String bundle = pkg.replace('.', '/') + "/messages/messages_en_US.properties";
    try (InputStream in = source.open(bundle)) {
      if (in != null) {
        props.load(new InputStreamReader(in, StandardCharsets.UTF_8));
      }
    } catch (Exception ignored) {
      // an unreadable bundle simply leaves the i18n key visible, which is the loudest signal
    }
    return props;
  }

  /**
   * One place classes and resources are read from: a module's {@code target/classes} while the
   * reactor is building it, or its jar once it has been installed. Both turn up on the classpath
   * and both have to be readable the same way.
   */
  record Source(Path path, boolean jar) {

    static Source of(Path path) {
      if (Files.isDirectory(path)) return new Source(path, false);
      if (Files.isRegularFile(path) && path.toString().endsWith(".jar")) {
        return new Source(path, true);
      }
      return null;
    }

    /** The entry's contents, or null when this source does not hold it. */
    InputStream open(String entry) throws IOException {
      if (!jar) {
        Path file = path.resolve(entry);
        return Files.exists(file) ? Files.newInputStream(file) : null;
      }
      // Read the one entry out into memory: the caller closes the stream, not the archive.
      try (ZipFile zip = new ZipFile(path.toFile())) {
        ZipEntry zipEntry = zip.getEntry(entry);
        if (zipEntry == null) return null;
        try (InputStream in = zip.getInputStream(zipEntry)) {
          return new ByteArrayInputStream(in.readAllBytes());
        }
      } catch (IOException e) {
        return null;
      }
    }
  }

  static String translate(String value, Properties msgs) {
    if (value == null || !value.startsWith("i18n:")) return value == null ? "" : value;
    String[] parts = value.split(":");
    if (parts.length != 3) return value;
    return format(msgs.getProperty(parts[2], value));
  }

  /**
   * Bundle values reach the user through {@code MessageFormat}, which Hop applies whether or not
   * there are parameters. Running it here too is what makes a doubled apostrophe and a quoted
   * '${VARIABLE}' read the same on the page as in the dialog.
   */
  static String format(String value) {
    try {
      return MessageFormat.format(value, new Object[0]);
    } catch (IllegalArgumentException e) {
      // Not a format pattern after all - the raw value is the best answer available.
      return value;
    }
  }

  static String str(AnnotationInstance ai, String name, String dflt) {
    AnnotationValue v = ai.value(name);
    return v == null ? dflt : v.asString();
  }

  static String render(Plugin p) {
    return LICENSE + GENERATED_MARKER + "\n// Edit those, not this file.\n\n" + body(p);
  }

  static String body(Plugin p) {
    StringBuilder sb = new StringBuilder();
    if (!p.configKey().isEmpty()) {
      sb.append("These options are stored under ")
          .append(literal(p.configKey()))
          .append(" in `hop-config.json`.\n\n");
    }
    sb.append("[%header, cols=\"2,5,1\"]\n|===\n|Option|Description|Default\n\n");
    for (Option o : p.options()) {
      sb.append("|").append(text(o.label())).append("\n");
      String description = flatten(o.toolTip());
      if (o.variables() && acceptsVariables(o.type())) {
        description =
            sentence(description) + (description.isEmpty() ? "" : " ") + "Accepts variables.";
      }
      if (isLongDefault(o.dflt())) {
        // An AsciiDoc cell, so the default can be a block under the description where there is
        // room for it. The Default column is a narrow one and a long value wraps to a word a line.
        sb.append("a|")
            .append(text(description))
            .append("\n\n.Default\n[listing]\n----\n")
            .append(o.dflt().strip())
            .append("\n----\n");
        sb.append("|_(with the description)_\n\n");
      } else {
        sb.append("|").append(text(description)).append("\n");
        sb.append("|").append(o.dflt().isEmpty() ? "" : literal(o.dflt())).append("\n\n");
      }
    }
    sb.append("|===\n");
    return sb.toString();
  }

  /**
   * A checkbox is built as a plain SWT {@code Button}, which has nowhere to put a variable, so its
   * {@code variables()} flag never reaches the user however it is set. Only the widgets backed by a
   * text field really take one.
   */
  static boolean acceptsVariables(String type) {
    return switch (type) {
      case "TEXT", "MULTI_LINE_TEXT", "FILENAME", "FOLDER", "COMBO", "METADATA" -> true;
      default -> false;
    };
  }

  /** Rounds off a tooltip so a sentence appended after it does not run on. */
  static String sentence(String s) {
    if (s.isEmpty() || s.endsWith(".") || s.endsWith("!") || s.endsWith("?")) return s;
    return s + ".";
  }

  /** Collapses a label or tooltip onto the single line a table cell is written on. */
  static String flatten(String s) {
    return s == null ? "" : s.replace("\n", " ").replace("\r", " ").replaceAll(" +", " ").trim();
  }

  /**
   * A label or tooltip as a table cell. These are prose written for a dialog, not AsciiDoc, and
   * they contain things AsciiDoc would otherwise read as markup: {@code **} in a glob, {@code
   * ${VARIABLE}} and {@code {NAME}} in an example, a {@code |} anywhere at all. {@code pass:c[]}
   * applies character substitution - so {@code <}, {@code >} and {@code &} still reach the page
   * safely - and no other substitution, which leaves every one of those alone. Escaping them one at
   * a time is what produced visible backslashes in the first version of this file.
   */
  static String text(String s) {
    String flat = flatten(s);
    return flat.isEmpty() ? "" : "pass:c[" + flat.replace("]", "\\]") + "]";
  }

  /**
   * Whether a default is too big for the Default column. Most are a word or a number; a few - the
   * AI Assistant's standing prompt, for one - are paragraphs, and those belong in a block rather
   * than in a column an eighth of the table wide.
   */
  static boolean isLongDefault(String s) {
    return s.length() > 80 || s.contains("\n");
  }

  /**
   * A default value as monospaced text. The {@code `+...+`} form looks like the one to reach for
   * but it is a passthrough, so nothing inside it is substituted - including the {@code {plus}} a
   * literal {@code +} would have to be written as. {@code pass:c[]} inside the backticks keeps the
   * monospace and leaves the value alone.
   */
  static String literal(String s) {
    return "`pass:c[" + flatten(s).replace("]", "\\]") + "]`";
  }
}
