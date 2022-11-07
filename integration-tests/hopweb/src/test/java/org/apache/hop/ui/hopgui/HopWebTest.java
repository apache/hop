package org.apache.hop.ui.hopgui;


import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;
import org.openqa.selenium.By;
import org.openqa.selenium.Dimension;
import org.openqa.selenium.Keys;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;


public class HopWebTest {

    private static int MARGIN_TOP = 35;
    private static int MARGIN_LEFT = 10;

    private static int WAIT_SHORT = 1000;
    private static int WAIT_MEDIUM = 1000;
    private static int WAIT_LONG = 2000;
    private static int  WAIT_VERY_LONG = 5000;
    private static WebDriver driver;
    private static Actions actions;
    private WebElement element;
    private static WebDriverWait wait;
    private WebElement contextCanvas, pipelineCanvas;
    private Dimension canvasDimension;
    private boolean pipelineCreated = false;

    private static File driverFile;
    private static boolean isHeadless;
    private static String baseUrl, transformName;
    private static String transformsFile;
    private static ScreenshotUtil screenshotUtil;

/*
    public static void main(String[] args){
        if(args.length != 4){
            System.out.println("Usage: HopWebTest <path to driver file> <base url> <transforms file> <headless>");
            System.out.println("example: HopWebTest ./chromedriver http://localhost:8080/ui ./transforms.csv true");
            System.exit(1);
        }
        if(!StringUtils.isBlank(args[0])){
            driverFile = new File(args[0]);
        }else{
            driverFile = new File(HopWebTest.class.getClassLoader().getResource("chromedriver").getFile());
        }

        if(!StringUtils.isBlank(args[1])){
            baseUrl = args[1];
        }else{
            baseUrl = "http://localhost:8080/ui";
        }

        if(!StringUtils.isBlank(args[2])){
            transformsFile = args[2];
        }else{
            transformsFile = "./transforms.csv";
        }

        if(!StringUtils.isBlank(args[3]) && (args[3] == "true" || args[3] == "false")){
            isHeadless = Boolean.valueOf(args[3]);
        }else{
            isHeadless = true;
        }
    }
*/

    @BeforeAll
    public static void setUp() throws Exception {
        screenshotUtil = new ScreenshotUtil();

        driverFile = new File(HopWebTest.class.getClassLoader().getResource("chromedriver").getFile());
        baseUrl = "http://localhost:8080/ui";
        transformsFile = "./transforms.csv";
        isHeadless = true;

        System.setProperty("webdriver.chrome.driver", driverFile.getAbsolutePath());
        ChromeOptions options = new ChromeOptions();
        if (isHeadless) {
            options.addArguments("headless");
        }
        options.addArguments("--window-size=1280,800");
        driver = new ChromeDriver(options);
        actions = new Actions(driver);
        wait = new WebDriverWait(driver, Duration.ofSeconds(3));
        driver.get(baseUrl);
        driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(3));

        // check if the help dialog is shown. If it is, close it.
        checkHelpDialog();

    }

    @Test
    @Order(2)
    public void testNewPipeline() throws InterruptedException {

        // Create a new pipeline
        clickElement("/html/body/div[2]/div[3]/div[2]/div");
        clickFirstCanvasElement(getGraphCanvas());
        WebDriverWait wait = new WebDriverWait(driver,Duration.ofSeconds(5));
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath("//div[text()='New pipeline']")));
        assertEquals(1, driver.findElements(By.xpath("//div[text()='New pipeline']")).size());
        Thread.sleep(WAIT_VERY_LONG);
    }

    @ParameterizedTest(name = "{index} => transform=''{0}''" )
    @CsvFileSource(resources = "/transforms.csv")
    @Order(3)
    public void testAddTransform(String transformName) throws InterruptedException, IOException {
        createNewTransform(transformName);
    }

    /**
     * Check if the Hop welcome dialog is shown.
     * If it is, check if it is benign, then close it.
     *
     * @throws InterruptedException
     */
    private static void checkHelpDialog() throws InterruptedException, IOException {

        try{
            WebElement welcomeCloseElement = driver.findElement(By.xpath("//div[text()='" + "Apache Hop" + "']/../div[5]"));

            if(welcomeCloseElement != null){
                new Actions(driver).moveToElement(welcomeCloseElement).click().perform();
            }
        }catch(NoSuchElementException e){
            screenshotUtil.takeScreenshot(driver, "close-help-dialog-failed.png");
        }
    }

    private void closeAndcreateNewPipeline() throws InterruptedException, IOException {
        driver.navigate().refresh();

        checkHelpDialog();

        testNewPipeline();
    }

    public void createNewTransform(String transformName) throws InterruptedException, IOException {

        this.transformName = transformName;
        pipelineCanvas = getGraphCanvas();

        clickCanvasAtPos(pipelineCanvas, 0, 0);

        contextCanvas =  getContextCanvas();
        sendInput(transformName);
        clickFirstCanvasElement(contextCanvas);

        Thread.sleep(WAIT_SHORT);

        clickCanvasAtPos(pipelineCanvas, 10, 10);

        Thread.sleep(WAIT_SHORT);
        sendInput("edit");

        clickFirstCanvasElement(getContextCanvas());

        Thread.sleep(WAIT_SHORT);

        try {
            // can we pick up the transform's dialog?
            String elementHtml = getElementHtml(transformName);
            assertNotNull(elementHtml);
            Thread.sleep(WAIT_SHORT);

        }catch(Exception e) {
            restartOnFailure(e);
        }

        try{

            // close the dialog
            driver.findElement(By.xpath("//body")).sendKeys(Keys.ESCAPE);

            Thread.sleep(WAIT_SHORT);

            assertNull(getElementHtml(transformName));

            Thread.sleep(WAIT_SHORT);

            // remove the transform
            new Actions(driver).moveToElement(pipelineCanvas, 10,10).click().pause(Duration.ofMillis(WAIT_SHORT)).perform();

            Thread.sleep(WAIT_SHORT);

            int nbCanvasElements = driver.findElements(By.xpath("//canvas")).size();

            contextCanvas = getContextCanvas();

            sendInput("delete");

            clickFirstCanvasElement(contextCanvas);

            Thread.sleep(WAIT_SHORT);

            // check if the context dialog was closed correctly.
            assertEquals(1, driver.findElements(By.xpath("//canvas")).size());

        }catch(Exception e){
            restartOnFailure(e);
        }
    }

    private String getElementHtml(String transformName){
        try{
            WebElement transformNameElement = driver.findElement(By.xpath("//div[text()='" + transformName + "']"));
            WebElement transformElement = transformNameElement.findElement(By.xpath("./../.."));

            String transformHtml = transformElement.getAttribute("innerHTML");

            return transformHtml;

        }catch(Exception e){
            screenshotUtil.takeScreenshot(driver, "get-element-html-" + transformName + ".png");
        }
        return null;
    }

    private void clickElement(String xpath) {
        WebElement element = driver.findElement(By.xpath(xpath));
        new Actions(driver).moveToElement(element).click().perform();
    }

    private void clickFirstCanvasElement(WebElement canvas) throws InterruptedException {

        canvasDimension = canvas.getSize();

        int posX = 0-(canvasDimension.getWidth()/2)+MARGIN_LEFT;
        int posY = 0-(canvasDimension.getHeight()/2)+MARGIN_TOP;

        clickCanvasAtPos(canvas, posX, posY);

    }

    private void clickCanvasAtPos(WebElement canvas, int posX, int posY) {
        try{
            new Actions(driver).moveToElement(canvas, posX, posY).click().pause(Duration.ofMillis(WAIT_SHORT)).perform();
        }catch(Exception e){
            screenshotUtil.takeScreenshot(driver, transformName + "-canvas-click" + posX + "-" + posY + ".png");
        }
    }

    private WebElement getCanvas(int canvasNb){
        try{
            return driver.findElements(By.xpath("//canvas")).get(canvasNb);
        }catch(Exception e){
            screenshotUtil.takeScreenshot(driver, "get-canvas-for-" + transformName + ".png");
//            fail();
        }
        return null;
    }

    private WebElement getGraphCanvas(){
        // pipeline and workflow canvas is the first one in the dom.
        return getCanvas(0);
    }
    private WebElement getContextCanvas() throws InterruptedException, IOException {
        // context canvas is the second one in the dom.
        return getCanvas(1);
    }

    @AfterAll
    public static void tearDown() {
        driver.quit();
    }

    public void sendInput(String inputString) throws InterruptedException, IOException {
        // safe to assume the input we need is the last one added.
        List<WebElement> inputElements = driver.findElements(By.tagName("input"));
        WebElement inputElement = inputElements.get(inputElements.size()-1);
        try{
            inputElement.sendKeys(inputString);
        }catch(Exception e){
            restartOnFailure(e);
        }
        Thread.sleep(WAIT_SHORT);
    }
    private void restartOnFailure(Exception e) throws InterruptedException, IOException {
        e.printStackTrace();
        closeAndcreateNewPipeline();
        fail();
    }
}
