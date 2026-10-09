/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.pipeline.transforms.watchfiles;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.xml.stream.XMLOutputFactory;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.discovery.ClassNameFilter;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

/** Runs the existing JUnit suites from an exported classpath without keeping Maven in memory. */
public class WatchFilesTestLauncher {
  public static void main(String[] args) throws Exception {
    Path reports = Path.of(args[0]);
    Files.createDirectories(reports);
    String suite = args.length > 1 ? args[1] : "load";
    var request = LauncherDiscoveryRequestBuilder.request();
    if ("load".equals(suite) || "gui".equals(suite)) {
      request.selectors(
          DiscoverySelectors.selectClass(
              "org.apache.hop.pipeline.transforms.watchfiles."
                  + ("load".equals(suite) ? "WatchFilesLoadTest" : "WatchFilesDialogTest")));
    } else if ("headless".equals(suite)) {
      request
          .selectors(
              DiscoverySelectors.selectPackage("org.apache.hop.pipeline.transforms.watchfiles"))
          .filters(
              ClassNameFilter.includeClassNamePatterns(".*Test"),
              ClassNameFilter.excludeClassNamePatterns(".*DialogTest", ".*LoadTest"));
    } else {
      throw new IllegalArgumentException("Suite must be load, headless or gui");
    }
    SummaryGeneratingListener summary = new SummaryGeneratingListener();
    Map<String, TestRecord> cases = new ConcurrentHashMap<>();
    Map<String, Long> started = new ConcurrentHashMap<>();
    TestExecutionListener results =
        new TestExecutionListener() {
          @Override
          public void testPlanExecutionStarted(TestPlan plan) {
            for (var root : plan.getRoots()) {
              for (var test : plan.getDescendants(root)) {
                if (test.isTest())
                  cases.put(test.getUniqueId(), record(test, "skipped", "Not executed", 0));
              }
            }
          }

          @Override
          public void executionStarted(TestIdentifier test) {
            if (test.isTest()) started.put(test.getUniqueId(), System.nanoTime());
          }

          @Override
          public void executionSkipped(TestIdentifier test, String reason) {
            if (test.isTest()) cases.put(test.getUniqueId(), record(test, "skipped", reason, 0));
          }

          @Override
          public void executionFinished(TestIdentifier test, TestExecutionResult result) {
            if (!test.isTest()) {
              result.getThrowable().ifPresent(error -> error.printStackTrace(System.err));
              return;
            }
            String state =
                switch (result.getStatus()) {
                  case SUCCESSFUL -> "passed";
                  case FAILED -> "failure";
                  case ABORTED -> "skipped";
                };
            String reason = result.getThrowable().map(WatchFilesTestLauncher::trace).orElse("");
            long elapsed =
                System.nanoTime() - started.getOrDefault(test.getUniqueId(), System.nanoTime());
            cases.put(test.getUniqueId(), record(test, state, reason, elapsed / 1_000_000_000.0));
          }
        };
    LauncherFactory.create().execute(request.build(), summary, results);
    var verdict = summary.getSummary();
    PrintWriter log = new PrintWriter(System.out, true);
    verdict.printTo(log);
    verdict.printFailuresTo(log);
    try (var stream = Files.newOutputStream(reports.resolve("TEST-watchfiles-" + suite + ".xml"))) {
      var xml = XMLOutputFactory.newFactory().createXMLStreamWriter(stream, "UTF-8");
      xml.writeStartDocument("UTF-8", "1.0");
      xml.writeStartElement("testsuite");
      xml.writeAttribute("name", "watchfiles-" + suite);
      xml.writeAttribute("tests", Long.toString(verdict.getTestsFoundCount()));
      xml.writeAttribute("failures", Long.toString(verdict.getTestsFailedCount()));
      xml.writeAttribute("errors", Long.toString(verdict.getContainersFailedCount()));
      xml.writeAttribute(
          "skipped",
          Long.toString(
              cases.values().stream().filter(test -> "skipped".equals(test.state())).count()));
      for (var test : cases.values()) {
        xml.writeStartElement("testcase");
        xml.writeAttribute("name", test.name());
        xml.writeAttribute("classname", test.className());
        xml.writeAttribute("time", Double.toString(test.seconds()));
        if (!"passed".equals(test.state())) {
          xml.writeStartElement(test.state());
          xml.writeCharacters(test.reason());
          xml.writeEndElement();
        }
        xml.writeEndElement();
      }
      if (verdict.getTotalFailureCount() > 0) {
        xml.writeStartElement("system-err");
        for (var failure : verdict.getFailures())
          xml.writeCharacters(trace(failure.getException()));
        xml.writeEndElement();
      }
      xml.writeEndElement();
      xml.writeEndDocument();
      xml.close();
    }
    System.exit(
        verdict.getTestsSucceededCount() > 0 && verdict.getTotalFailureCount() == 0 ? 0 : 1);
  }

  private static TestRecord record(
      TestIdentifier test, String state, String reason, double seconds) {
    String className =
        test.getSource()
            .filter(MethodSource.class::isInstance)
            .map(MethodSource.class::cast)
            .map(MethodSource::getClassName)
            .orElse("WatchFiles");
    return new TestRecord(test.getDisplayName(), className, state, reason, seconds);
  }

  private static String trace(Throwable error) {
    StringWriter text = new StringWriter();
    error.printStackTrace(new PrintWriter(text));
    return text.toString();
  }

  private record TestRecord(
      String name, String className, String state, String reason, double seconds) {}
}
