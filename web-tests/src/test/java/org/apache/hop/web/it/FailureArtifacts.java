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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Optional;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebDriver;

/**
 * Writes a screenshot and the rendered DOM for every failing test.
 *
 * <p>A failing UI test says almost nothing on its own, and the browser is gone by the time anyone
 * reads the report. Capturing this centrally also keeps the tests themselves free of the
 * try/catch-and-screenshot noise the earlier suite was built from.
 *
 * <p>This hooks {@code afterTestExecution} rather than {@code TestWatcher}: the latter runs after
 * the {@code @AfterEach} cleanup, which closes leftover dialogs and so photographs a GUI that has
 * already been tidied up.
 */
public class FailureArtifacts implements AfterTestExecutionCallback {

  @Override
  public void afterTestExecution(ExtensionContext context) {
    if (context.getExecutionException().isPresent()) {
      capture(context.getRequiredTestClass().getSimpleName() + "." + context.getDisplayName());
    }
  }

  private void capture(String name) {
    HopWebEnvironment environment = HopWebEnvironment.getIfStarted();
    if (environment == null) {
      // The failure was Hop Web not starting at all; there is no browser to photograph.
      return;
    }
    WebDriver driver = environment.getDriver();
    String base = name.replaceAll("[^A-Za-z0-9.-]+", "-").toLowerCase(Locale.ROOT);
    Path directory = Path.of(System.getProperty("hopweb.artifacts", "target/hopweb-artifacts"));
    try {
      Files.createDirectories(directory);
      Files.write(
          directory.resolve(base + ".png"),
          ((TakesScreenshot) driver).getScreenshotAs(OutputType.BYTES));
      Files.writeString(directory.resolve(base + ".html"), dom(driver), StandardCharsets.UTF_8);
      System.out.println("Failure artifacts written to " + directory.resolve(base) + ".{png,html}");
    } catch (IOException e) {
      System.out.println("Could not write failure artifacts for " + name + ": " + e.getMessage());
    }
  }

  /** The rendered DOM, not the page source: RAP builds the entire UI client side. */
  private String dom(WebDriver driver) {
    return Optional.ofNullable(
            (String)
                ((JavascriptExecutor) driver)
                    .executeScript("return document.documentElement.outerHTML"))
        .orElse("");
  }
}
