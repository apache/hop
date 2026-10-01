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

package org.apache.hop.web.it;

import java.util.List;
import java.util.logging.Level;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.logging.LogEntries;
import org.openqa.selenium.logging.LogEntry;
import org.openqa.selenium.logging.LogType;

/**
 * The other half of {@link ServerLog}: what went wrong in the browser.
 *
 * <p>Hop Web is a Java UI painted by a JavaScript client, and the two fail in different places. A
 * server side crash never reaches the page; a client side one - RAP asking for a widget the server
 * has already forgotten, an image that will not load, a protocol message it cannot apply - never
 * reaches the server log. Reading both means a test does not have to assert on the specific thing
 * that broke to notice that something did.
 */
public final class BrowserConsole {

  /**
   * Errors the browser reports that say nothing about Hop.
   *
   * <p>Kept as short as possible. Every entry here is a hole in the check, so an entry has to
   * describe something Hop cannot fix rather than something it has not fixed yet.
   */
  private static final List<String> NOISE =
      List.of(
          // Chrome asks for one whether the application serves one or not.
          "favicon.ico",
          // RAP probes for browser features and expects some of the probes to fail.
          "Failed to load resource: net::ERR_",
          // Headless Chrome has no fonts to preload from and says so on every page load.
          "Failed to decode downloaded font",
          "OTS parsing error");

  private BrowserConsole() {}

  /**
   * Errors logged by the browser since the last call, worst first.
   *
   * <p>Reading the log drains it, which is what makes "since the last call" work: a test marks the
   * start by draining, and reads at the end.
   *
   * <p>Returns nothing at all when the driver cannot serve browser logs. That is not a failure to
   * report - a browser that does not implement the (non-standard) log endpoint would otherwise fail
   * every test in the suite for a reason that has nothing to do with Hop.
   */
  public static List<String> errors(WebDriver driver) {
    LogEntries entries = read(driver);
    if (entries == null) {
      return List.of();
    }
    return entries.getAll().stream()
        .filter(entry -> entry.getLevel().intValue() >= Level.SEVERE.intValue())
        .map(LogEntry::getMessage)
        .filter(message -> NOISE.stream().noneMatch(message::contains))
        .distinct()
        .toList();
  }

  /** Throws away whatever is buffered, so the next read only covers what happens after this. */
  public static void drain(WebDriver driver) {
    errors(driver);
  }

  /**
   * Whether this browser hands its console over at all.
   *
   * <p>Worth asking before concluding anything from an empty result: a browser that does not
   * implement the endpoint reports no errors for the same reason a healthy one does.
   */
  public static boolean isSupported(WebDriver driver) {
    return read(driver) != null;
  }

  private static LogEntries read(WebDriver driver) {
    try {
      return driver.manage().logs().get(LogType.BROWSER);
    } catch (RuntimeException e) {
      // The log endpoint is not part of WebDriver proper, so a browser is entitled to refuse it.
      return null;
    }
  }
}
