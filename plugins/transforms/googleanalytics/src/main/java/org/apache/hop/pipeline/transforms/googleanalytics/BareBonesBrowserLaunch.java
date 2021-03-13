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

package org.apache.hop.pipeline.transforms.googleanalytics;

import java.util.Arrays;

public class BareBonesBrowserLaunch {

    static final String[] browsers =
            {
                    "google-chrome", "firefox", "opera", "epiphany", "konqueror", "conkeror", "midori", "kazehakase",
                    "mozilla" };
    static final String errMsg = "Error attempting to launch web browser";

    /**
     * Opens the specified web page in the user's default browser
     *
     * @param url
     *          A web address (URL) of a web page (ex: "http://www.google.com/")
     */
    public static void openURL( String url ) {
        try { // attempt to use Desktop library from JDK 1.6+
            Class<?> d = Class.forName( "java.awt.Desktop" );
            d.getDeclaredMethod( "browse", new Class<?>[] { java.net.URI.class } ).invoke(
                    d.getDeclaredMethod( "getDesktop" ).invoke( null ), new Object[] { java.net.URI.create( url ) } );
            // above code mimicks: java.awt.Desktop.getDesktop().browse()
        } catch ( Exception ignore ) { // library not available or failed
            String osName = System.getProperty( "os.name" );
            try {
                if ( osName.startsWith( "Mac OS" ) ) {
                    Class
                            .forName( "com.apple.eio.FileManager" ).getDeclaredMethod( "openURL", new Class<?>[] { String.class } )
                            .invoke( null, new Object[] { url } );
                } else if ( osName.startsWith( "Windows" ) ) {
                    Runtime.getRuntime().exec( "rundll32 url.dll,FileProtocolHandler " + url );
                } else { // assume Unix or Linux
                    String browser = null;
                    for ( String b : browsers ) {
                        if ( browser == null
                                && Runtime.getRuntime().exec( new String[] { "which", b } ).getInputStream().read() != -1 ) {
                            Runtime.getRuntime().exec( new String[] { browser = b, url } );
                        }
                    }
                    if ( browser == null ) {
                        throw new Exception( Arrays.toString( browsers ) );
                    }
                }
            } catch ( Exception e ) {
                // silently fail
                // JOptionPane.showMessageDialog(null, errMsg + "\n" + e.toString());
                e.printStackTrace();
            }
        }
    }

}
