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
        File errorImageFile = new File("target/images/" +screenshotPath);
        try{
            FileUtils.copyFile(imgAtError, errorImageFile);
        }catch(IOException e){
            e.printStackTrace();
        }
    }
}
