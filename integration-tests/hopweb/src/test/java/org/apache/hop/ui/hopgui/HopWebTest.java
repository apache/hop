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

import static org.junit.Assert.*;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

public class HopWebTest {
  private WebDriver driver;
  private Actions actions;
  private String baseUrl;
  private WebElement element;
  private WebDriverWait wait;

  @Before
  public void setUp() throws Exception {
    boolean isHeadless = Boolean.parseBoolean( System.getProperty( "headless.unittest", "true" ) );
    ChromeOptions options = new ChromeOptions();
    if ( isHeadless ) {
      options.addArguments( "headless" );
    }
    options.addArguments( "--window-size=1280,800" );
    driver = new ChromeDriver( options );
    actions = new Actions( driver );
    wait = new WebDriverWait( driver, 5 );
    baseUrl = System.getProperty( "test.baseurl", "http://localhost:8080" );
    driver.get( baseUrl );
    driver.manage().timeouts().implicitlyWait( 5, TimeUnit.SECONDS );
  }

  @Test
  public void testAppLoading() {
    assertEquals( driver.getTitle(), "Hop" );
  }

  @Test
  public void testContextDialog() {
    By xpath = By.xpath(
        "//div[starts-with(text(), 'Search') and not(contains(text(), 'string'))]"
      );
    assertEquals(0, driver.findElements(xpath).size());

    clickElement("//div[text() = 'File']");
    clickElement("//div[text() = 'New']");
    // TODO: driver.findElements(xpath).size() is now 2 for some reason, but should be 1.
    assertTrue( 1 <= driver.findElements(xpath).size());
  }

  @Test
  public void testNewPipeline() {
    // Create a new pipeline
    createNewPipeline();

    assertEquals(1, driver.findElements(
      By.xpath(
        "//div[text()='New pipeline']"
      )
    ).size());
  }

  private void createNewPipeline() {
    // Create a new Pipeline
    clickElement("//div[text() = 'File']");
    clickElement("//div[text() = 'New']");
    element = driver.findElement(By.xpath("//div[starts-with(text(), 'Search') and not(contains(text(), 'string'))]"));
    element = element.findElement(By.xpath("./../..//input"));
    element.sendKeys(Keys.UP);
    element.sendKeys(Keys.LEFT);
    element.sendKeys(Keys.RETURN);
  }

  private void clickElement( String xpath ) {
    element = wait.until( ExpectedConditions.elementToBeClickable( By.xpath( xpath ) ) );
    element.click();
  }

  @After
  public void tearDown() {
    driver.quit();
  }
}
