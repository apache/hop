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

package org.apache.hop.core.plugins.isolation;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Stream;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.gui.plugin.GuiPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginRegistry;

/**
 * Starts Hop on an install that holds one plugin, and proves that plugin works there on its own.
 *
 * <p>Run by {@code tools/check-plugin-classpath.sh --isolated --runtime}, with {@code
 * HOP_PLUGIN_BASE_FOLDERS} pointing at the plugins of that install and its lib/core on the
 * classpath. A target is a plugin folder or, for a plugin that ships in lib/core, the plugin jar.
 * For each target it checks that:
 *
 * <ul>
 *   <li>at least one plugin registers from it,
 *   <li>the main class of every such plugin loads and, outside the GUI plugin types, instantiates,
 *   <li>every class in the plugin's own jars links: its fields, methods and constructors resolve
 *       against what the plugin class loader can see.
 * </ul>
 *
 * <p>The last one is what a plugin with a missing dependency fails: a class that only resolved
 * because a different plugin put a jar in lib/core. jdeps finds most of these at package level;
 * this finds the rest, and the plugins that silently register nothing.
 *
 * <p>Without SWT on the classpath (a headless install, #9501) a class that only misses GUI types,
 * such as a dialog, is reported as {@code GUI} and is not a failure. A plugin's main class that
 * needs the GUI is: it breaks every headless run.
 *
 * <p>Lines starting with {@code ISOLATION} are the result, the rest is Hop's own logging. Exit code
 * 0 when nothing failed, 1 when something did, 2 when Hop did not start.
 */
@SuppressWarnings("java:S106") // command line tool: the console is its user interface
public class PluginIsolationCheck {

  static final String PREFIX = "ISOLATION ";

  /** Plugin types whose main class needs a running GUI to be constructed. */
  private static final Set<String> GUI_PLUGIN_TYPES =
      Set.of(
          "org.apache.hop.core.gui.plugin.GuiPluginType",
          "org.apache.hop.ui.hopgui.perspective.HopPerspectivePluginType",
          "org.apache.hop.ui.hopgui.file.HopFileTypePluginType",
          "org.apache.hop.ui.core.gui.SearchableAnalyserPluginType");

  /** Packages of hop-ui, hop-ui-rcp, SWT and the Eclipse platform bundles they need. */
  private static final List<String> GUI_PACKAGES =
      List.of(
          "org.apache.hop.ui.",
          "org.eclipse.swt.",
          "org.eclipse.jface.",
          "org.eclipse.rap.",
          "org.eclipse.e4.",
          "org.eclipse.core.commands.",
          "org.eclipse.core.runtime.",
          "org.eclipse.equinox.",
          "org.eclipse.osgi.",
          "org.eclipse.text.",
          "org.eclipse.tm4e.",
          "org.eclipse.ui.");

  /** Classes hop-ui and hop-ui-rcp put in packages of hop-core. */
  private static final Set<String> GUI_CLASSES_IN_CORE_PACKAGES =
      Set.of(
          "org.apache.hop.core.ProgressMonitorAdapter",
          "org.apache.hop.core.SwtDesktopDpiImages",
          "org.apache.hop.core.SwtUniversalImage",
          "org.apache.hop.core.SwtUniversalImageBitmap",
          "org.apache.hop.core.SwtUniversalImageSvg");

  /**
   * Plugin types that are also used for GUI hooks: one that needs the GUI to instantiate is only
   * ever called from the GUI.
   */
  private static final Set<String> MIXED_PLUGIN_TYPES =
      Set.of("org.apache.hop.core.extension.ExtensionPointPluginType");

  private final PrintStream out;
  private final List<Path> folders = new ArrayList<>();
  private final List<Path> jars = new ArrayList<>();
  private boolean headless;
  private int failures;

  PluginIsolationCheck(PrintStream out) {
    this.out = out;
  }

  public static void main(String[] args) {
    // Keep a handle on the real stdout: plugins may redirect System.out.
    PrintStream out = System.out;
    PluginIsolationCheck check = new PluginIsolationCheck(out);
    for (int i = 0; i < args.length; i++) {
      if ("--folder".equals(args[i]) && i + 1 < args.length) {
        check.folders.add(Path.of(args[++i]).toAbsolutePath().normalize());
      } else if ("--jar".equals(args[i]) && i + 1 < args.length) {
        check.jars.add(Path.of(args[++i]).toAbsolutePath().normalize());
      } else {
        out.println(PREFIX + "ERROR unknown argument: " + args[i]);
        System.exit(2);
      }
    }
    if (check.folders.isEmpty() && check.jars.isEmpty()) {
      out.println(PREFIX + "ERROR nothing to check: pass --folder <dir> or --jar <file>");
      System.exit(2);
    }

    try {
      HopEnvironment.init();
      check.headless = !isSwtAvailable();
      initGuiPluginTypes(out);
    } catch (Throwable e) {
      out.println(PREFIX + "ERROR Hop did not start: " + rootCause(e));
      // The whole trace goes to the log next to the install, see check-plugin-classpath.sh.
      e.printStackTrace(System.err);
      System.exit(2);
    }
    out.println(PREFIX + "INFO " + (check.headless ? "headless, without SWT" : "with SWT"));

    int exit = check.run() ? 0 : 1;
    // Plugins can leave non-daemon threads behind.
    System.exit(exit);
  }

  /** True when SWT is on the classpath. Without it an install is headless (#9501). */
  private static boolean isSwtAvailable() {
    try {
      Class.forName("org.eclipse.swt.SWT", false, PluginIsolationCheck.class.getClassLoader());
      return true;
    } catch (ClassNotFoundException | LinkageError e) {
      return false;
    }
  }

  /**
   * Registers the GUI plugin types, without a display. A plugin that only contributes GUI elements
   * registers nothing otherwise. With hop-ui on the classpath that is HopGuiEnvironment.init(),
   * called by reflection so this class runs on an install without hop-ui too. When hop-ui is not
   * there, or its GUI plugin types cannot be registered without SWT, the GuiPluginType of hop-core
   * is registered on its own.
   */
  private static void initGuiPluginTypes(PrintStream out) throws Exception {
    try {
      Class.forName("org.apache.hop.ui.hopgui.HopGuiEnvironment").getMethod("init").invoke(null);
      return;
    } catch (ClassNotFoundException e) {
      // No hop-ui at all.
    } catch (InvocationTargetException | LinkageError e) {
      out.println(
          PREFIX + "INFO the GUI plugin types of hop-ui cannot be registered: " + rootCause(e));
    }
    PluginRegistry.getInstance().registerType(GuiPluginType.getInstance());
  }

  boolean run() {
    PluginRegistry registry = PluginRegistry.getInstance();
    Map<Path, List<IPlugin>> byTarget = new LinkedHashMap<>();
    folders.forEach(folder -> byTarget.put(folder, new ArrayList<>()));
    jars.forEach(jar -> byTarget.put(jar, new ArrayList<>()));

    for (Class<? extends IPluginType> type : registry.getPluginTypes()) {
      for (IPlugin plugin : registry.getPlugins(type)) {
        Path target = targetOf(plugin);
        if (target != null) {
          byTarget.get(target).add(plugin);
        }
      }
    }

    for (Map.Entry<Path, List<IPlugin>> entry : byTarget.entrySet()) {
      Path target = entry.getKey();
      List<IPlugin> plugins = entry.getValue();
      out.println(PREFIX + "INFO " + target + ": " + plugins.size() + " plugin(s)");
      for (IPlugin plugin : plugins) {
        checkPlugin(registry, target, plugin);
      }
      int guiOnly = checkLinkage(target, classLoadersFor(registry, target, plugins));
      if (plugins.isEmpty()) {
        if (headless && guiOnly > 0) {
          // Perspectives and other hop-ui plugin types do not exist without hop-ui.
          out.println(PREFIX + "GUI " + target + ": registers nothing without SWT");
        } else {
          fail(target, "no plugin registers from it");
        }
      }
    }
    return failures == 0;
  }

  /** The target a plugin comes from, or null when it is not one of the plugins under test. */
  private Path targetOf(IPlugin plugin) {
    if (!plugin.isNativePlugin()) {
      URL directory = plugin.getPluginDirectory();
      if (directory == null) {
        return null;
      }
      Path path = toPath(directory);
      return folders.contains(path) ? path : null;
    }
    if (jars.isEmpty() || plugin.getClassMap().isEmpty()) {
      return null;
    }
    String className = plugin.getClassMap().values().iterator().next();
    URL resource =
        PluginIsolationCheck.class
            .getClassLoader()
            .getResource(className.replace('.', '/') + ".class");
    if (resource == null || !"jar".equals(resource.getProtocol())) {
      return null;
    }
    String file = resource.getPath();
    int separator = file.indexOf("!/");
    if (separator < 0) {
      return null;
    }
    try {
      Path jar = Path.of(new URI(file.substring(0, separator))).toAbsolutePath().normalize();
      return jars.contains(jar) ? jar : null;
    } catch (URISyntaxException | IllegalArgumentException e) {
      return null;
    }
  }

  private void checkPlugin(PluginRegistry registry, Path target, IPlugin plugin) {
    String type = plugin.getPluginType().getName();
    String label = plugin.getPluginType().getSimpleName() + " " + plugin.getIds()[0];
    boolean guiType = GUI_PLUGIN_TYPES.contains(type);
    try {
      if (guiType) {
        ClassLoader classLoader = registry.getClassLoader(plugin);
        for (String className : plugin.getClassMap().values()) {
          Class.forName(className, false, classLoader);
        }
      } else {
        registry.loadClass(plugin);
      }
    } catch (Throwable e) {
      Throwable cause = rootCause(e);
      if (!isMissingClass(cause)) {
        // A constructor that needs more than a bare environment is not a packaging problem.
        out.println(PREFIX + "WARN " + target + ": " + label + " does not instantiate: " + cause);
      } else if (headless
          && (guiType || MIXED_PLUGIN_TYPES.contains(type))
          && isGuiClass(missingName(cause))) {
        out.println(PREFIX + "GUI " + target + ": " + label + " needs " + missingName(cause));
      } else if (headless && isGuiClass(missingName(cause))) {
        fail(target, label + " needs the GUI to load: missing " + missingName(cause));
      } else {
        fail(target, label + ": missing " + missingName(cause));
      }
    }
  }

  /**
   * Loads and links every class in the target's own jars.
   *
   * @return the number of classes that only miss GUI classes, on a headless install
   */
  private int checkLinkage(Path target, List<ClassLoader> classLoaders) {
    // Missing class -> the classes that need it
    Map<String, Set<String>> missing = new TreeMap<>();
    Set<String> guiOnly = new TreeSet<>();
    for (Path jar : ownJars(target)) {
      for (String className : classNames(jar)) {
        Throwable cause = link(className, classLoaders);
        if (cause != null) {
          if (!isMissingClass(cause)) {
            out.println(PREFIX + "WARN " + target + ": " + className + ": " + cause);
          } else if (headless && isGuiClass(missingName(cause))) {
            guiOnly.add(className);
          } else {
            missing.computeIfAbsent(missingName(cause), k -> new TreeSet<>()).add(className);
          }
        }
      }
    }
    for (Map.Entry<String, Set<String>> entry : missing.entrySet()) {
      fail(target, "missing " + entry.getKey() + ", needed by " + example(entry.getValue()));
    }
    // The classes that need hop-ui to load, for check-plugin-classpath.sh: a reference into hop-ui
    // from any other class is code that runs without hop-ui and fails there.
    guiOnly.forEach(className -> out.println(PREFIX + "GUICLASS " + className));
    if (!guiOnly.isEmpty()) {
      out.println(
          PREFIX
              + "GUI "
              + target
              + ": "
              + guiOnly.size()
              + " class(es) need SWT, e.g. "
              + guiOnly.iterator().next());
    }
    return guiOnly.size();
  }

  /**
   * Loads and links a class with the first class loader that can.
   *
   * @return null when it links, else why it did not with the last class loader tried
   */
  private static Throwable link(String className, List<ClassLoader> classLoaders) {
    Throwable last = null;
    for (ClassLoader classLoader : classLoaders) {
      try {
        Class<?> clazz = Class.forName(className, false, classLoader);
        clazz.getDeclaredFields();
        clazz.getDeclaredMethods();
        clazz.getDeclaredConstructors();
        return null;
      } catch (Throwable e) {
        last = rootCause(e);
      }
    }
    return last;
  }

  /**
   * The class loaders of the target's plugins. Plugins in one folder can have different ones: a
   * database plugin gets the shared JDBC folders, the bulk loader transform next to it does not. A
   * class of the folder links when one of them can link it. For a target that registers nothing, a
   * class loader over its jars and lib/, so its classes can still be checked.
   */
  private List<ClassLoader> classLoadersFor(
      PluginRegistry registry, Path target, List<IPlugin> plugins) {
    List<ClassLoader> classLoaders = new ArrayList<>();
    for (IPlugin plugin : plugins) {
      try {
        ClassLoader classLoader = registry.getClassLoader(plugin);
        if (!classLoaders.contains(classLoader)) {
          classLoaders.add(classLoader);
        }
      } catch (Throwable e) {
        out.println(PREFIX + "WARN " + target + ": no class loader for a plugin: " + rootCause(e));
      }
    }
    if (!classLoaders.isEmpty()) {
      return classLoaders;
    }
    List<URL> urls = new ArrayList<>();
    List<Path> candidates = new ArrayList<>(ownJars(target));
    if (Files.isDirectory(target.resolve("lib"))) {
      try (Stream<Path> files = Files.walk(target.resolve("lib"))) {
        files.filter(path -> path.toString().endsWith(".jar")).forEach(candidates::add);
      } catch (IOException e) {
        // Checked without lib/ then.
      }
    }
    for (Path path : candidates) {
      try {
        urls.add(path.toUri().toURL());
      } catch (MalformedURLException e) {
        // Not a usable path; the jar is skipped.
      }
    }
    classLoaders.add(
        new URLClassLoader(urls.toArray(new URL[0]), PluginIsolationCheck.class.getClassLoader()));
    return classLoaders;
  }

  /** A plugin folder's own jars are the ones directly in it; its dependencies are in lib/. */
  private static List<Path> ownJars(Path target) {
    if (Files.isRegularFile(target)) {
      return List.of(target);
    }
    try (Stream<Path> files = Files.list(target)) {
      return files.filter(path -> path.toString().endsWith(".jar")).sorted().toList();
    } catch (IOException e) {
      return List.of();
    }
  }

  private static List<String> classNames(Path jar) {
    List<String> names = new ArrayList<>();
    try (JarFile jarFile = new JarFile(jar.toFile())) {
      Enumeration<JarEntry> entries = jarFile.entries();
      while (entries.hasMoreElements()) {
        String name = entries.nextElement().getName();
        if (name.endsWith(".class")
            && !name.startsWith("META-INF/")
            && !name.endsWith("module-info.class")
            && !name.endsWith("package-info.class")) {
          names.add(name.substring(0, name.length() - ".class".length()).replace('/', '.'));
        }
      }
    } catch (IOException e) {
      // An unreadable jar fails on the class loader side too.
    }
    return names;
  }

  private void fail(Path target, String message) {
    failures++;
    out.println(PREFIX + "FAIL " + target + ": " + message);
  }

  private static String example(Set<String> classes) {
    return classes.iterator().next()
        + (classes.size() > 1 ? " and " + (classes.size() - 1) + " more" : "");
  }

  private static boolean isGuiClass(String className) {
    int nested = className.indexOf('$');
    String outer = nested < 0 ? className : className.substring(0, nested);
    return GUI_CLASSES_IN_CORE_PACKAGES.contains(outer)
        || GUI_PACKAGES.stream().anyMatch(className::startsWith);
  }

  private static boolean isMissingClass(Throwable e) {
    return e instanceof NoClassDefFoundError || e instanceof ClassNotFoundException;
  }

  /** The missing class of a NoClassDefFoundError or ClassNotFoundException, dotted. */
  private static String missingName(Throwable e) {
    return String.valueOf(e.getMessage()).replace('/', '.').trim();
  }

  private static Throwable rootCause(Throwable e) {
    Throwable cause = e;
    while (cause.getCause() != null && cause.getCause() != cause) {
      cause = cause.getCause();
    }
    return cause;
  }

  private static Path toPath(URL url) {
    try {
      return new File(url.toURI()).toPath().toAbsolutePath().normalize();
    } catch (URISyntaxException | IllegalArgumentException e) {
      return Path.of(url.getPath()).toAbsolutePath().normalize();
    }
  }
}
