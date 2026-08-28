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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.apache.hop.web.it.pages.HopGuiPage;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.BrowserWebDriverContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.ToStringConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * Owns the Hop Web container and the browser for the whole JVM.
 *
 * <p>Both are expensive to create - a cold Hop Web needs the better part of a minute to build its
 * GUI on the first request - so they are created once and shared by every test class.
 * Testcontainers reaps them when the JVM exits.
 *
 * <p>System properties (see the module pom for defaults):
 *
 * <ul>
 *   <li>{@code hopweb.image} - image under test, default {@code apache/hop-web:Development}
 *   <li>{@code hopweb.build} - build the image from this working tree first, for changes that are
 *       not committed yet. Overrides {@code hopweb.image}.
 *   <li>{@code hopweb.pull} - re-pull the image first, default true. Leave it on: the Development
 *       tag is rebuilt from main daily, and a cached copy silently tests whatever was pulled weeks
 *       ago. Re-pulling an unchanged image transfers nothing.
 *   <li>{@code hopweb.url} - URL of an already running Hop Web. Set it while writing tests to skip
 *       container startup entirely, e.g. {@code -Dhopweb.url=http://localhost:8080/ui}
 *   <li>{@code hopweb.browser} - {@code auto} (default), {@code container} or {@code local}
 *   <li>{@code hopweb.browserImage} - the browser image, for a containerised browser
 *   <li>{@code hopweb.headless} - only meaningful for a local browser
 * </ul>
 */
public final class HopWebEnvironment {

  private static final String LOCAL_IMAGE_VERSION = "local";
  private static final String LOCAL_IMAGE = "hop-web:" + LOCAL_IMAGE_VERSION;

  /**
   * The browser image. Pinned here rather than left to Testcontainers, which derives the tag from
   * the Selenium client on the classpath: the selenium/standalone-* images are published a release
   * behind that client, so every client bump would fail the build on a manifest that does not exist
   * yet ({@code 404 manifest for selenium/standalone-chrome:4.48.0 not found}). Bump this when the
   * matching image is out; the client and the browser only have to speak the same WebDriver
   * protocol, not carry the same version.
   */
  private static final String DEFAULT_BROWSER_IMAGE = "selenium/standalone-chrome:4.47.0";

  private static final String HOP_WEB_ALIAS = "hop-web";
  private static final int HOP_WEB_PORT = 8080;

  /** Where the container keeps the configuration Hop Web reads at startup. */
  private static final String CONFIG_PATH = "/usr/local/tomcat/webapps/ROOT/config/hop-config.json";

  private static HopWebEnvironment instance;

  private final String uiUrl;
  private final WebDriver driver;

  /**
   * Everything Hop Web has printed, streamed as it runs. Null when the tests drive a Hop Web that
   * was started outside them, where there is no log to watch.
   */
  private ToStringConsumer serverLog;

  private boolean uiOpened;

  private HopWebEnvironment() {
    Network network = Network.newNetwork();
    String externalUrl = property("hopweb.url", "");
    BrowserMode browserMode = BrowserMode.resolve(property("hopweb.browser", "auto"));

    String urlForBrowser;
    if (!externalUrl.isBlank()) {
      urlForBrowser = adaptExternalUrl(externalUrl, browserMode);
    } else {
      GenericContainer<?> hopWeb = startHopWeb(network);
      urlForBrowser =
          browserMode == BrowserMode.CONTAINER
              ? "http://" + HOP_WEB_ALIAS + ":" + HOP_WEB_PORT + "/ui"
              : "http://" + hopWeb.getHost() + ":" + hopWeb.getMappedPort(HOP_WEB_PORT) + "/ui";
    }

    this.uiUrl = urlForBrowser;
    this.driver = browserMode == BrowserMode.CONTAINER ? containerBrowser(network) : localBrowser();
  }

  public static synchronized HopWebEnvironment get() {
    if (instance == null) {
      instance = new HopWebEnvironment();
      // Testcontainers reaps what it started, but a locally launched ChromeDriver would
      // outlive the Surefire JVM and leave a browser behind on the build agent.
      Runtime.getRuntime().addShutdownHook(new Thread(instance::close));
    }
    return instance;
  }

  /** The environment if it was ever started, so failure handling cannot start one itself. */
  public static synchronized HopWebEnvironment getIfStarted() {
    return instance;
  }

  private void close() {
    try {
      driver.quit();
    } catch (RuntimeException e) {
      // Nothing useful left to do while the JVM is going down.
    }
  }

  public WebDriver getDriver() {
    return driver;
  }

  /**
   * Everything Hop Web has logged so far, or empty when the tests are pointed at a Hop Web they did
   * not start and so cannot read.
   */
  public String serverLog() {
    return serverLog == null ? "" : serverLog.toUtf8String();
  }

  /**
   * Reloads the Hop GUI in a new RAP session, abandoning whatever state the old one was stuck in.
   * Expensive, so it is a recovery path rather than something to do between tests.
   */
  public void reopenUi() {
    uiOpened = false;
    openUi();
  }

  /**
   * Loads the Hop GUI once per JVM and waits until it is actually usable.
   *
   * <p>A cold Hop Web answers {@code /ui} with HTTP 200 long before the GUI exists: the RAP entry
   * point only builds it on the first request, which means scanning the plugin registry. Waiting
   * for the main toolbar is what "started" really means here - the previous version of these tests
   * slept for two minutes instead, which was both slower and less reliable.
   */
  public void openUi() {
    if (uiOpened) {
      return;
    }
    long start = System.currentTimeMillis();
    driver.get(uiUrl);
    HopGuiPage.waitFor(driver, Duration.ofSeconds(startupTimeoutSeconds()))
        .until(ExpectedConditions.presenceOfElementLocated(HopGuiPage.NEW_FILE));
    System.out.println("Hop GUI ready in " + (System.currentTimeMillis() - start) + " ms");
    uiOpened = true;
  }

  /** The Hop Web URL as the browser has to address it, which is not always how we address it. */
  public String getUiUrl() {
    return uiUrl;
  }

  private GenericContainer<?> startHopWeb(Network network) {
    GenericContainer<?> container =
        new GenericContainer<>(DockerImageName.parse(imageUnderTest()))
            .withNetwork(network)
            .withNetworkAliases(HOP_WEB_ALIAS)
            .withExposedPorts(HOP_WEB_PORT)
            // Hop Web only reports the version through the GUI, so log which image actually ran:
            // a daily job testing a published tag must not go green against a stale image.
            .withImagePullPolicy(
                shouldPull() ? PullPolicy.alwaysPull() : PullPolicy.defaultPolicy())
            .withCopyFileToContainer(
                MountableFile.forClasspathResource("hop-config.json"), CONFIG_PATH)
            .waitingFor(Wait.forHttp("/ui").forStatusCode(200))
            .withStartupTimeout(Duration.ofSeconds(startupTimeoutSeconds()));
    container.start();
    serverLog = new ToStringConsumer();
    container.followOutput(serverLog);
    System.out.println(
        "Hop Web container started from image "
            + container.getDockerImageName()
            + " (id "
            + container.getContainerId()
            + ")");
    return container;
  }

  /**
   * The image to test: the published one by default, or one built from this working tree when
   * {@code -Dhopweb.build=true} is given.
   *
   * <p>Building is for developing against changes that are not committed yet. The daily job stays
   * on the published tag deliberately - that is the Hop Web people actually run, and rebuilding it
   * nightly would be testing the build rather than the product.
   */
  private String imageUnderTest() {
    if (buildsLocally()) {
      buildLocalImage();
      return LOCAL_IMAGE;
    }
    return property("hopweb.image", "apache/hop-web:Development");
  }

  private static boolean buildsLocally() {
    return Boolean.parseBoolean(property("hopweb.build", "false"));
  }

  /** Pull the published image, but never a tag that only exists on this machine. */
  private static boolean shouldPull() {
    return !buildsLocally() && Boolean.parseBoolean(property("hopweb.pull", "true"));
  }

  /**
   * Builds Hop Web from this working tree, through the project's own image script rather than a
   * second Dockerfile that would drift away from it.
   *
   * <p>The fast builder packages artifacts the Maven build already produced, so build the
   * assemblies first with {@code mvn clean install -DskipTests}.
   */
  private void buildLocalImage() {
    Path repository = Path.of(property("hopweb.repository", ".."));
    List<String> command =
        List.of(
            "./docker/build-hop-images.sh",
            "--images",
            "web",
            "--builder",
            "fast",
            "--version",
            LOCAL_IMAGE_VERSION);
    System.out.println("Building " + LOCAL_IMAGE + " from " + repository.toAbsolutePath());
    try {
      Process process =
          new ProcessBuilder(command)
              .directory(repository.toFile())
              .redirectErrorStream(true)
              .start();
      try (BufferedReader output =
          new BufferedReader(new InputStreamReader(process.getInputStream(), UTF_8))) {
        output.lines().forEach(System.out::println);
      }
      int status = process.waitFor();
      if (status != 0) {
        throw new IllegalStateException(
            "Building "
                + LOCAL_IMAGE
                + " failed with exit code "
                + status
                + ". The fast builder packages what the Maven build produced, so run"
                + " 'mvn clean install -DskipTests' first.");
      }
    } catch (IOException e) {
      throw new IllegalStateException("Could not run " + command.get(0), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while building " + LOCAL_IMAGE, e);
    }
  }

  private WebDriver containerBrowser(Network network) {
    BrowserWebDriverContainer<?> browser =
        new BrowserWebDriverContainer<>(
                DockerImageName.parse(property("hopweb.browserImage", DEFAULT_BROWSER_IMAGE)))
            .withNetwork(network)
            .withCapabilities(chromeOptions())
            .withStartupTimeout(Duration.ofSeconds(startupTimeoutSeconds()));
    browser.start();
    return browser.getWebDriver();
  }

  private WebDriver localBrowser() {
    // Selenium Manager resolves a matching chromedriver by itself since Selenium 4.6.
    return new ChromeDriver(chromeOptions());
  }

  private ChromeOptions chromeOptions() {
    ChromeOptions options = new ChromeOptions();
    if (Boolean.parseBoolean(property("hopweb.headless", "true"))) {
      options.addArguments("--headless=new");
    }
    options.addArguments("--no-sandbox");
    options.addArguments("--disable-dev-shm-usage");
    options.addArguments("--window-size=1600,1000");
    options.addArguments("--remote-allow-origins=*");
    return options;
  }

  /** A browser in a container cannot reach the host's localhost without a little help. */
  private String adaptExternalUrl(String externalUrl, BrowserMode browserMode) {
    if (browserMode != BrowserMode.CONTAINER) {
      return externalUrl;
    }
    URI uri = URI.create(externalUrl);
    if (!"localhost".equals(uri.getHost()) && !"127.0.0.1".equals(uri.getHost())) {
      return externalUrl;
    }
    int port = uri.getPort() < 0 ? 80 : uri.getPort();
    Testcontainers.exposeHostPorts(port);
    return externalUrl.replaceFirst(
        "//" + uri.getHost() + ":" + port, "//host.testcontainers.internal:" + port);
  }

  private static long startupTimeoutSeconds() {
    return Long.parseLong(property("hopweb.startupTimeout", "300"));
  }

  private static String property(String name, String defaultValue) {
    String value = System.getProperty(name);
    return value == null || value.isBlank() ? defaultValue : value;
  }

  private enum BrowserMode {
    CONTAINER,
    LOCAL;

    static BrowserMode resolve(String value) {
      if ("auto".equalsIgnoreCase(value)) {
        // The selenium/standalone-chrome images are amd64 only, so an arm64 developer machine
        // has to drive the browser it already has.
        return "aarch64".equals(System.getProperty("os.arch")) ? LOCAL : CONTAINER;
      }
      return "local".equalsIgnoreCase(value) ? LOCAL : CONTAINER;
    }
  }
}
