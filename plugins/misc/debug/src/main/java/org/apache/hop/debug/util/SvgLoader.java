package org.apache.hop.debug.util;

import org.apache.batik.transcoder.Transcoder;
import org.apache.batik.transcoder.TranscoderInput;
import org.apache.batik.transcoder.TranscoderOutput;
import org.apache.batik.transcoder.image.PNGTranscoder;
import org.apache.hop.core.exception.HopException;

import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

public class SvgLoader {

  public static BufferedImage transcodeSVGDocument( ClassLoader classLoader, String filename, int width, int height ) throws HopException {
    // Create a PNG transcoder.
    Transcoder t = new PNGTranscoder();

    // Set the transcoding hints.
    t.addTranscodingHint( PNGTranscoder.KEY_WIDTH, new Float( width ) );
    t.addTranscodingHint( PNGTranscoder.KEY_HEIGHT, new Float( height ) );

    // Create the transcoder input.
    //
    InputStream inputStream = classLoader.getResourceAsStream( filename );
    TranscoderInput input = new TranscoderInput( inputStream );

    ByteArrayOutputStream ostream = null;
    try {
      // Create the transcoder output.
      ostream = new ByteArrayOutputStream();
      TranscoderOutput output = new TranscoderOutput( ostream );

      // Save the image.
      t.transcode( input, output );

      // Flush and close the stream.
      ostream.flush();
      ostream.close();
    } catch ( Exception ex ) {
      throw new HopException( "Error loading svg file " + filename, ex );
    }

    // Convert the byte stream into an image.
    byte[] imgData = ostream.toByteArray();
    Image img = Toolkit.getDefaultToolkit().createImage( imgData );

    // Wait until the entire image is loaded.
    MediaTracker tracker = new MediaTracker( new JPanel() );
    tracker.addImage( img, 0 );
    try {
      tracker.waitForID( 0 );
    } catch ( InterruptedException ex ) {
      throw new HopException( "Interrupted", ex );
    }

    // Return the newly rendered image.
    //
    if ( img instanceof BufferedImage ) {
      return (BufferedImage) img;
    }

    // Convert to a buffered image for the Hop GC API
    //
    BufferedImage bufferedImage = new BufferedImage( img.getWidth( null ), img.getHeight( null ), BufferedImage.TYPE_INT_ARGB );
    Graphics2D bufferedGc = bufferedImage.createGraphics();
    bufferedGc.drawImage( img, 0, 0, null );
    bufferedGc.dispose();
    return bufferedImage;
  }

}
