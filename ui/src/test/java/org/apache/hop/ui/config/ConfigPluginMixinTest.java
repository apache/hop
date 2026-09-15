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
package org.apache.hop.ui.config;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.lang.annotation.Annotation;
import java.lang.reflect.AccessibleObject;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Guards the {@code hop} command line against a configuration plugin it cannot mix in.
 *
 * <p>{@code Hop.addMixinPlugins} hands every {@link ConfigPlugin} of the requested category to
 * picocli as a mixin, and picocli refuses an object that declares no {@code @Command},
 * {@code @Option}, {@code @Parameters} or {@code @Unmatched}. One such plugin does not degrade the
 * command line, it stops it starting at all - every subcommand, not just the one that owns the
 * plugin. That is easy to introduce from the GUI side, where a config plugin is a settings tab and
 * is never exercised by a command, and it is invisible until somebody runs the command line.
 */
public class ConfigPluginMixinTest {

  @Test
  public void testEveryConfigPluginCanBeMixedIntoTheCommandLine() throws Exception {
    List<Class<?>> offenders = new ArrayList<>();
    for (Class<?> candidate : classesInThisModule()) {
      if (candidate.getAnnotation(ConfigPlugin.class) == null) {
        continue;
      }
      if (!declaresSomethingPicocliAccepts(candidate)) {
        offenders.add(candidate);
      }
    }

    assertTrue(
        offenders.isEmpty(),
        "These are annotated @ConfigPlugin but declare nothing picocli accepts, which breaks"
            + " every hop command: "
            + offenders
            + ". Give the plugin real @CommandLine.Option fields, or drop @ConfigPlugin if its"
            + " settings are only reachable from the GUI.");
  }

  /** The rule picocli applies in {@code CommandReflection.validateCommandSpec}. */
  private boolean declaresSomethingPicocliAccepts(Class<?> candidate) {
    if (candidate.getAnnotation(CommandLine.Command.class) != null) {
      return true;
    }
    for (Class<?> type = candidate;
        type != null && type != Object.class;
        type = type.getSuperclass()) {
      if (Stream.<AccessibleObject[]>of(type.getDeclaredFields(), type.getDeclaredMethods())
          .flatMap(Stream::of)
          .anyMatch(this::isPicocliMember)) {
        return true;
      }
    }
    return false;
  }

  private boolean isPicocliMember(AccessibleObject member) {
    for (Annotation annotation : member.getAnnotations()) {
      Class<?> type = annotation.annotationType();
      if (type == CommandLine.Option.class
          || type == CommandLine.Parameters.class
          || type == CommandLine.Unmatched.class
          || type == CommandLine.Mixin.class) {
        return true;
      }
    }
    return false;
  }

  /**
   * Every class this module compiled. Loaded without running static initialisers, so a class that
   * would need a display or a plugin registry is still safe to look at.
   */
  private List<Class<?>> classesInThisModule() throws Exception {
    Path root = moduleClassesRoot();
    List<Class<?>> classes = new ArrayList<>();
    try (Stream<Path> paths = Files.walk(root)) {
      for (Path path :
          (Iterable<Path>) paths.filter(p -> p.toString().endsWith(".class"))::iterator) {
        String name =
            root.relativize(path)
                .toString()
                .replace(File.separatorChar, '.')
                .replaceAll("\\.class$", "");
        try {
          classes.add(Class.forName(name, false, getClass().getClassLoader()));
        } catch (Throwable e) {
          // A class that cannot even be loaded is not a config plugin we can judge.
        }
      }
    }
    return classes;
  }

  private Path moduleClassesRoot() throws URISyntaxException {
    return Path.of(
        org.apache.hop.ui.hopgui.HopGui.class
            .getProtectionDomain()
            .getCodeSource()
            .getLocation()
            .toURI());
  }
}
