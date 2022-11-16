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

import org.apache.commons.io.FileUtils;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebDriver;

import java.io.File;
import java.io.IOException;

public class ScreenshotUtil {

    public ScreenshotUtil(){
    }

    public void takeScreenshot(WebDriver driver, String screenshotPath) {
        TakesScreenshot screenshot = (TakesScreenshot) driver;
        File imgAtError = screenshot.getScreenshotAs(OutputType.FILE);
        File errorImageFile = new File("target/images/" +screenshotPath.replaceAll(" ", "-").toLowerCase());
        try{
            FileUtils.copyFile(imgAtError, errorImageFile);
        }catch(IOException e){
            e.printStackTrace();
        }
    }
}
