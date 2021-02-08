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

package org.apache.hop.pipeline.transforms.mqtt.key;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

public class SSLSocketFactoryGenerator {

  public static SSLSocketFactory getSocketFactory( String caCrtFile, String crtFile, String keyFile, String password )
      throws Exception {

    char[] passwordCharArray = password == null ? new char[0] : password.toCharArray();

    // Security.addProvider( new BouncyCastleProvider() );
    CertificateFactory cf = CertificateFactory.getInstance( "X.509" );

    X509Certificate
        caCert =
        (X509Certificate) cf
            .generateCertificate( new ByteArrayInputStream( Files.readAllBytes( Paths.get( caCrtFile ) ) ) );

    X509Certificate
        cert =
        (X509Certificate) cf
            .generateCertificate( new ByteArrayInputStream( Files.readAllBytes( Paths.get( crtFile ) ) ) );

    File privateKeyFile = new File( keyFile );

    PrivateKey privateKey = PrivateKeyReader.readKey( privateKeyFile );

    KeyStore caKeyStore = KeyStore.getInstance( KeyStore.getDefaultType() );
    caKeyStore.load( null, null );
    caKeyStore.setCertificateEntry( "ca-certificate", caCert );
    TrustManagerFactory
        trustManagerFactory =
        TrustManagerFactory.getInstance( TrustManagerFactory.getDefaultAlgorithm() );
    trustManagerFactory.init( caKeyStore );

    KeyStore keyStore = KeyStore.getInstance( KeyStore.getDefaultType() );
    keyStore.load( null, null );
    keyStore.setCertificateEntry( "certificate", cert );
    //keyStore.setKeyEntry( "private-key", kp.getPrivate(), passwordCharArray,
    keyStore.setKeyEntry( "private-key", privateKey, passwordCharArray, new java.security.cert.Certificate[] { cert } );
    KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance( KeyManagerFactory.getDefaultAlgorithm() );
    keyManagerFactory.init( keyStore, passwordCharArray );

    // SSLContext context = SSLContext.getInstance("TLSv1");
    SSLContext context = SSLContext.getInstance( "TLSv1.2" );
    context.init( keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null );

    return context.getSocketFactory();

  }
}
