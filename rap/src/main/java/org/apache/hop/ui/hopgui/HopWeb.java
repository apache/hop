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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.batik.transcoder.TranscoderException;
import org.apache.batik.transcoder.TranscoderInput;
import org.apache.batik.transcoder.TranscoderOutput;
import org.apache.batik.transcoder.image.PNGTranscoder;
import org.apache.commons.io.FilenameUtils;
import org.eclipse.rap.rwt.application.Application;
import org.eclipse.rap.rwt.application.ApplicationConfiguration;
import org.eclipse.rap.rwt.client.WebClient;
import org.eclipse.rap.rwt.service.ResourceLoader;

public class HopWeb implements ApplicationConfiguration {

  public void configure( Application application ) {
    application.addResource("ui/images/logo_icon.png", new ResourceLoader() {
      public InputStream getResourceAsStream( String resourceName ) throws IOException {
        // Convert svg to png without Display
        PNGTranscoder t = new PNGTranscoder();
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("ui/images/logo_icon.svg");
        TranscoderInput input = new TranscoderInput(inputStream);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        TranscoderOutput output = new TranscoderOutput(outputStream);
        try {
          t.transcode(input, output);
        } catch (TranscoderException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        return new ByteArrayInputStream(outputStream.toByteArray());
      }
    });
    Arrays.asList(
      "org/apache/hop/ui/hopgui/clipboard.js"
    ).stream().forEach( str -> {
      application.addResource( "js/" + FilenameUtils.getName( str ), new ResourceLoader() {
        @Override
        public InputStream getResourceAsStream( String resourceName ) throws IOException {
          return this.getClass().getClassLoader().getResourceAsStream( str );
        }
      } );
    });
    Map<String, String> properties = new HashMap<String, String>();
    properties.put( WebClient.PAGE_TITLE, "Hop" );
    properties.put( WebClient.FAVICON, "ui/images/logo_icon.png" );
    application.addEntryPoint( "/ui", HopWebEntryPoint.class, properties );
    application.setOperationMode( Application.OperationMode.SWT_COMPATIBILITY );

    application.addServiceHandler( "downloadServiceHandler", new DownloadServiceHandler() );
  }
}
