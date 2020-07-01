package org.apache.hop.core.gui;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.svg.HopSvgGraphics2D;
import org.apache.hop.core.svg.SvgFile;
import org.junit.Before;
import org.junit.Test;

import java.io.FileOutputStream;

public class SvgGcTest {

  @Before
  public void before() throws Exception {
    HopEnvironment.init();
  }

  @Test
  public void testDrawImage() throws Exception {
    HopSvgGraphics2D graphics2D = HopSvgGraphics2D.newDocument();
    SvgGc gc = new SvgGc( graphics2D, new Point(500, 500), 32, 0, 0 );

    gc.setForeground( IGc.EColor.LIGHTGRAY );
    gc.drawRectangle( 250, 50, 63, 64 );
    gc.drawImage( new SvgFile("test.svg", getClass().getClassLoader()), 250, 50, 32, 32, 2f, Math.PI/2 ); // 45 degrees rotation

    String xml = graphics2D.toXml();
    System.out.println(xml);
    FileOutputStream outputStream = new FileOutputStream( "/tmp/test.svg" );
    outputStream.write( xml.getBytes("UTF-8") );
    outputStream.flush();
    outputStream.close();

  }

}