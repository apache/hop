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

package org.apache.hop.airflow.launcher;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * A workflow of the project that becomes a DAG. Read with the JDK XML parser: listing the DAGs runs
 * every 30 seconds or so and must not start Hop.
 *
 * @param relativePath the path of the .hwf file, relative to the project, with forward slashes
 * @param dagId the DAG id
 * @param description the description of the workflow, or a reason why it could not be read
 * @param schedule a cron expression from the Start action's schedule, or null
 */
public record HopWorkflowFile(
    String relativePath, String dagId, String description, String schedule) {

  /** The value of {@code schedulerType} in the Start action. */
  static final int INTERVAL = 1;

  static final int DAILY = 2;
  static final int WEEKLY = 3;
  static final int MONTHLY = 4;

  /**
   * The workflows of the project that match one of the patterns, or all of them without patterns.
   * Hidden folders such as .git are skipped.
   */
  public static List<HopWorkflowFile> scan(Path project, List<String> patterns, String dagIdPrefix)
      throws IOException {
    List<PathMatcher> matchers = new ArrayList<>();
    for (String pattern : patterns) {
      matchers.add(FileSystems.getDefault().getPathMatcher("glob:" + pattern));
    }
    List<Path> files = new ArrayList<>();
    Files.walkFileTree(
        project,
        new SimpleFileVisitor<>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attributes) {
            boolean hidden = !dir.equals(project) && dir.getFileName().toString().startsWith(".");
            return hidden ? FileVisitResult.SKIP_SUBTREE : FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) {
            if (file.getFileName().toString().endsWith(".hwf")) {
              Path relative = project.relativize(file);
              if (matchers.isEmpty() || matchers.stream().anyMatch(m -> m.matches(relative))) {
                files.add(file);
              }
            }
            return FileVisitResult.CONTINUE;
          }
        });
    List<HopWorkflowFile> workflows = new ArrayList<>();
    for (Path file : files.stream().sorted().toList()) {
      String relative = project.relativize(file).toString().replace('\\', '/');
      workflows.add(read(file, relative, dagIdPrefix));
    }
    return workflows;
  }

  static HopWorkflowFile read(Path file, String relative, String dagIdPrefix) {
    String description;
    String schedule = null;
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
      factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
      Document document = factory.newDocumentBuilder().parse(file.toFile());
      Element root = document.getDocumentElement();
      description = text(root, "description");
      Element actions = child(root, "actions");
      if (actions != null) {
        for (Node node = actions.getFirstChild(); node != null; node = node.getNextSibling()) {
          if (node instanceof Element action
              && "SPECIAL".equals(text(action, "type"))
              && "Y".equals(text(action, "repeat"))) {
            schedule = cron(action);
          }
        }
      }
    } catch (Exception e) {
      description = "Hop could not read this workflow: " + e.getMessage();
    }
    return new HopWorkflowFile(relative, dagId(dagIdPrefix, relative), description, schedule);
  }

  /**
   * The Start action's schedule as a cron expression. Intervals map when they divide an hour or a
   * day; other intervals and values with variables do not, and the DAG then has no schedule.
   */
  static String cron(Element start) {
    try {
      int minutes = Integer.parseInt(text(start, "minutes"));
      int hour = Integer.parseInt(text(start, "hour"));
      return switch (Integer.parseInt(text(start, "schedulerType"))) {
        case INTERVAL -> intervalCron(start);
        case DAILY -> minutes + " " + hour + " * * *";
        case WEEKLY ->
            // Hop and cron both count the days of the week from Sunday = 0.
            minutes + " " + hour + " * * " + Integer.parseInt(text(start, "weekDay"));
        case MONTHLY ->
            minutes + " " + hour + " " + Integer.parseInt(text(start, "DayOfMonth")) + " * *";
        default -> null;
      };
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static String intervalCron(Element start) {
    int seconds = Integer.parseInt(text(start, "intervalSeconds"));
    int minutes = Integer.parseInt(text(start, "intervalMinutes"));
    if (seconds != 0 || minutes <= 0) {
      return null;
    }
    if (minutes < 60 && 60 % minutes == 0) {
      return "*/" + minutes + " * * * *";
    }
    if (minutes % 60 == 0 && 24 % (minutes / 60) == 0) {
      int hours = minutes / 60;
      return switch (hours) {
        case 1 -> "0 * * * *";
        case 24 -> "0 0 * * *";
        default -> "0 */" + hours + " * * *";
      };
    }
    return null;
  }

  /** The prefix and the path without .hwf; folders become two underscores. */
  static String dagId(String prefix, String relativePath) {
    String withoutExtension = relativePath.substring(0, relativePath.length() - ".hwf".length());
    return prefix + withoutExtension.replace("/", "__").replaceAll("[^A-Za-z0-9_.-]", "_");
  }

  private static Element child(Element parent, String tag) {
    for (Node node = parent.getFirstChild(); node != null; node = node.getNextSibling()) {
      if (node instanceof Element element && tag.equals(element.getTagName())) {
        return element;
      }
    }
    return null;
  }

  private static String text(Element parent, String tag) {
    Element element = child(parent, tag);
    return element == null ? "" : element.getTextContent().trim();
  }
}
