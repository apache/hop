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

package org.apache.hop.ui.hopgui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.github.bonigarcia.wdm.WebDriverManager;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;
import org.openqa.selenium.By;
import org.openqa.selenium.Dimension;
import org.openqa.selenium.ElementNotInteractableException;
import org.openqa.selenium.Keys;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.opentest4j.AssertionFailedError;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

public class HopWebTest {

  private static int MARGIN_TOP = 35;
  private static int MARGIN_LEFT = 10;
  private static int WAIT_SHORT = 2000;
  private static int WAIT_MEDIUM = 3000;
  private static int WAIT_LONG = 5000;
  private static int WAIT_VERY_LONG = 10000;
  private static WebDriver driver;
  private static Actions actions;
  private static WebDriverWait wait;
  private WebElement contextCanvas, pipelineCanvas;
  private Dimension canvasDimension;
  private static boolean isHeadless;
  private static String baseUrl, transformName;
  private static String transformsFile;
  private static ScreenshotUtil screenshotUtil;
  private static GenericContainer<?> hopWebContainer;

  @BeforeAll
  public static void setUp() throws Exception {
    hopWebContainer =
        new GenericContainer<>(
                new ImageFromDockerfile()
                    .withFileFromClasspath("Dockerfile", "docker/Dockerfile.web")
                    .withFileFromClasspath(
                        "resources/hop-config.json", "docker/resources/hop-config.json"))
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/ui"));
    hopWebContainer.start();

    screenshotUtil = new ScreenshotUtil();

    InputStream input =
        new FileInputStream(
            HopWebTest.class.getClassLoader().getResource("hopwebtest.properties").getFile());
    Properties properties = new Properties();
    properties.load(input);
    baseUrl =
        "http://" + hopWebContainer.getHost() + ":" + hopWebContainer.getFirstMappedPort() + "/ui";
    System.out.println("Connection URL used: " + baseUrl);
    isHeadless = Boolean.valueOf(properties.getProperty("headless"));
    transformsFile = properties.getProperty("transformsFile");

    System.setProperty("webdriver.chrome.whitelistedIps", "");

    // Use webDriverManager to fetch correct chromedriver
    WebDriverManager.chromedriver().setup();

    ChromeOptions options = new ChromeOptions();
    if (isHeadless) {
      options.addArguments("headless");
    }
    options.addArguments("--no-sandbox");
    options.addArguments("--disable-dev-shm-usage");
    options.addArguments("--window-size=1280,800");
    options.addArguments("--remote-allow-origins=*");

    driver = new ChromeDriver(options);
    actions = new Actions(driver);
    wait = new WebDriverWait(driver, Duration.ofSeconds(3));
    driver.manage().timeouts().implicitlyWait(Duration.ofMillis(WAIT_SHORT));
    driver.get(baseUrl);

    // sleep for 2 minutes to give Hop Web plenty of time to fully start.
    Thread.sleep(Integer.valueOf(properties.getProperty("sleepStart")));

    // check if the help dialog is shown. If it is, close it.
    checkWelcomeDialog();
  }

  @AfterAll
  public static void cleanUp() throws Exception {
    hopWebContainer.stop();
  }

  @Test
  @Order(1)
  public void testNewPipeline() throws InterruptedException, IOException {
    // Create a new pipeline
    WebElement newImage = driver.findElement(By.id("toolbar-10010-new"));
    WebElement element = newImage.findElement(By.xpath("./../.."));
    new Actions(driver).moveToElement(element).click().perform();

    Thread.sleep(WAIT_SHORT);

    contextCanvas = getLastCanvas();

    assertNotNull(contextCanvas);

    clickFirstCanvasElement(contextCanvas);

    WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(5));
    wait.until(
        ExpectedConditions.visibilityOfElementLocated(By.xpath("//div[text()='New pipeline']")));
    assertEquals(1, driver.findElements(By.xpath("//div[text()='New pipeline']")).size());
    Thread.sleep(WAIT_LONG);
  }

  /**
   * Walk over the list of transforms and run tests for each of those. Implicit/explicit waits don't
   * really help here, so still resorting to Thread.sleep().
   *
   * @param transformName
   * @throws InterruptedException
   * @throws IOException
   */
  @ParameterizedTest(name = "{index} => transform=''{0}''")
  @CsvFileSource(files = "src/test/resources/transforms.csv")
  @Order(2)
  public void testAddTransform(String transformName) throws InterruptedException, IOException {
    System.out.println("#############################################################");
    System.out.println(transformName + " - Starting test");
    System.out.println("#############################################################");
    createNewTransform(transformName);
  }

  /**
   * Check if the Hop welcome dialog is shown. If it is, check if it is benign, then close it.
   *
   * @throws InterruptedException
   */
  private static void checkWelcomeDialog() throws InterruptedException, IOException {

    try {
      WebElement welcomeCloseElement =
          driver.findElement(By.xpath("//div[text()='" + "Apache Hop" + "']/../div[5]"));

      if (welcomeCloseElement != null) {
        new Actions(driver).moveToElement(welcomeCloseElement).click().perform();
      }
    } catch (NoSuchElementException e) {
      screenshotUtil.takeScreenshot(driver, "welcome-dialog-close-failed.png");
    }
  }

  private void closeAndcreateNewPipeline() throws InterruptedException, IOException {
    driver.navigate().refresh();

    checkWelcomeDialog();

    testNewPipeline();
  }

  public void createNewTransform(String transformName) throws InterruptedException, IOException {

    this.transformName = transformName;
    pipelineCanvas = getGraphCanvas();

    clickCanvasAtPos(pipelineCanvas, 0, 0);

    contextCanvas = getLastCanvas();

    sendInput(transformName);

    clickFirstCanvasElement(contextCanvas);

    Thread.sleep(WAIT_SHORT);

    clickCanvasAtPos(pipelineCanvas, 10, 10);

    Thread.sleep(WAIT_SHORT);
    sendInput("edit");

    contextCanvas = getLastCanvas();

    clickFirstCanvasElement(contextCanvas);

    // can we pick up the transform's dialog?
    try {
      System.out.println(transformName + " - Check if Dialog is open");
      assertTrue(isElementPresent(transformName));
    } catch (AssertionFailedError e) {
      restartOnFailure(e);
    }

    try {

      // close the dialog
      System.out.println(transformName + " - Closing dialog (Esc)");
      driver.findElement(By.xpath("//body")).sendKeys(Keys.ESCAPE);
      Thread.sleep(WAIT_SHORT);

      // for some transforms (e.g. Append Streams), the dialog is closed correctly, but the dialog
      // element keeps floating around, causing this assertion to fail.
      // check that we can't get the transform's dialog anymore
      System.out.println(transformName + " - Check if Dialog is gone");
      assertFalse(isElementPresent(transformName));
    } catch (AssertionFailedError e) {
      screenshotUtil.takeScreenshot(driver, transformName + "-could-not-get-dialog.png");
      restartOnFailure(e);
    }

    // open the context dialog again to delete this transform.
    try {

      // pick up the pipeline canvas again before we remove the transform
      pipelineCanvas = getCanvas(0);
      assertNotNull(pipelineCanvas);

      // now open the context dialog to remove the transform
      new Actions(driver)
          .moveToElement(pipelineCanvas, 10, 10)
          .click()
          .pause(Duration.ofMillis(WAIT_SHORT))
          .perform();

    } catch (AssertionFailedError e) {
      screenshotUtil.takeScreenshot(driver, transformName + "-could-not-get-delete-dialog.png");
      restartOnFailure(e);
    }

    try {

      System.out.println(transformName + " - Delete Transform");
      contextCanvas = getLastCanvas();

      sendInput("delete");

      clickFirstCanvasElement(contextCanvas);

      // check if the context dialog was closed correctly.
      // TODO: check if element is gone

    } catch (AssertionFailedError e) {
      screenshotUtil.takeScreenshot(driver, transformName + "-delete-dialog-not-closed.png");
      restartOnFailure(e);
    }
  }

  private String getElementHtml(String transformName) {
    try {
      WebElement transformNameElement =
          driver.findElement(By.xpath("//div[text()='" + transformName + "']"));
      WebElement transformElement = transformNameElement.findElement(By.xpath("./../.."));

      String transformHtml = transformElement.getAttribute("innerHTML");

      return transformHtml;

    } catch (Exception e) {
      e.printStackTrace();
      screenshotUtil.takeScreenshot(driver, transformName + "-get-element-html.png");
    }
    return null;
  }

  private boolean isElementPresent(String transformName) {
    try {
      String checkTransformNameTest =
          driver.findElement(By.xpath("//body/div[5]/div[1]")).getText();

      if (!checkTransformNameTest.equals(transformName)) {
        System.out.println(
            transformName + " - Dialog name does not match is : " + checkTransformNameTest);
        return false;
      }

      WebElement transformNameElement =
          driver.findElement(By.xpath("//div[text()='" + transformName + "']"));
      String transformHtml = transformNameElement.getAttribute("innerHTML");

      if (transformNameElement != null) {
        System.out.println(transformName + " - Element found");
        return true;
      } else {
        System.out.println(transformName + " - Element not found");
        return false;
      }
    } catch (NoSuchElementException e) {
      System.out.println(transformName + " - Element not found");
      return false;
    }
  }

  private void clickElementByXpath(String xpath) {
    WebElement element = driver.findElement(By.xpath(xpath));
    new Actions(driver).moveToElement(element).click().perform();
  }

  private void clickElementById(String id) {
    WebElement element = driver.findElement(By.id(id));
    new Actions(driver).moveToElement(element).click().perform();
  }

  private void clickFirstCanvasElement(WebElement canvas) throws InterruptedException {

    canvasDimension = canvas.getSize();

    int posX = 0 - (canvasDimension.getWidth() / 2) + MARGIN_LEFT;
    int posY = 0 - (canvasDimension.getHeight() / 2) + MARGIN_TOP;

    clickCanvasAtPos(canvas, posX, posY);
  }

  private void clickCanvasAtPos(WebElement canvas, int posX, int posY) {
    try {
      new Actions(driver)
          .moveToElement(canvas, posX, posY)
          .click()
          .pause(Duration.ofMillis(WAIT_SHORT))
          .perform();
    } catch (Exception e) {
      screenshotUtil.takeScreenshot(
          driver, transformName + "-canvas-click" + posX + "-" + posY + ".png");
    }
  }

  private WebElement getCanvas(int canvasNb) throws IOException, InterruptedException {
    try {
      WebElement canvas = driver.findElements(By.xpath("//canvas")).get(canvasNb);
      assertNotNull(canvas);
      return canvas;
    } catch (AssertionFailedError e) {
      screenshotUtil.takeScreenshot(driver, transformName + "-get-canvas.png");
      restartOnFailure(e);
    }
    return null;
  }

  private WebElement getGraphCanvas() throws IOException, InterruptedException {
    // pipeline and workflow canvas is the first one in the dom.
    return getCanvas(0);
  }

  private WebElement getContextCanvas() throws InterruptedException, IOException {
    // context canvas is the second one in the dom.
    return getCanvas(1);
  }

  private WebElement getLastCanvas() throws InterruptedException, IOException {
    List<WebElement> canvasList = driver.findElements(By.xpath("//canvas"));
    WebElement canvas = canvasList.get(canvasList.size() - 1);
    assertNotNull(canvas);
    return canvas;
  }

  @AfterAll
  public static void tearDown() {
    driver.quit();
  }

  public void sendInput(String inputString) throws InterruptedException, IOException {
    // safe to assume the input we need is the last one added.
    try {
      List<WebElement> inputElements = driver.findElements(By.tagName("input"));
      WebElement inputElement = inputElements.get(inputElements.size() - 1);
      assertTrue(inputElement.isDisplayed());
      Thread.sleep(WAIT_SHORT);
      inputElement.sendKeys(inputString);
      Thread.sleep(WAIT_SHORT);
    } catch (ElementNotInteractableException e) {
      e.printStackTrace();
      closeAndcreateNewPipeline();
      Thread.sleep(WAIT_LONG);
    } catch (AssertionFailedError e) {
      restartOnFailure(e);
      Thread.sleep(WAIT_LONG);
    }
  }

  private void restartOnFailure(AssertionFailedError e) throws InterruptedException, IOException {
    e.printStackTrace();
    closeAndcreateNewPipeline();
    Thread.sleep(WAIT_LONG);
    fail();
  }
}
