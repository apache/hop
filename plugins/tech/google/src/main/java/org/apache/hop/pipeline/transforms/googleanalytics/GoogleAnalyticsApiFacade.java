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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.analytics.Analytics;
import com.google.api.services.analytics.AnalyticsScopes;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.util.Assert;
import org.apache.hop.core.vfs.HopVfs;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;

public class GoogleAnalyticsApiFacade {

    private Analytics analytics;
    private final HttpTransport httpTransport;

    public static GoogleAnalyticsApiFacade createFor(
            String application, String oauthServiceAccount, String oauthKeyFile )
            throws GeneralSecurityException, IOException, HopFileException {

        return new GoogleAnalyticsApiFacade(
                GoogleNetHttpTransport.newTrustedTransport(),
                JacksonFactory.getDefaultInstance(),
                application,
                oauthServiceAccount,
                new File( HopVfs.getFileObject( oauthKeyFile ).getURL().getPath() )
        );
    }

    public GoogleAnalyticsApiFacade(HttpTransport httpTransport, JsonFactory jsonFactory, String application,
                                    String oathServiceEmail, File keyFile )
            throws IOException, GeneralSecurityException {

        Assert.assertNotNull( httpTransport, "HttpTransport cannot be null" );
        Assert.assertNotNull( jsonFactory, "JsonFactory cannot be null" );
        Assert.assertNotBlank( application, "Application name cannot be empty" );
        Assert.assertNotBlank( oathServiceEmail, "OAuth Service Email name cannot be empty" );
        Assert.assertNotNull( keyFile, "OAuth secret key file cannot be null" );

        this.httpTransport = httpTransport;

        Credential credential = new GoogleCredential.Builder()
                .setTransport( httpTransport )
                .setJsonFactory( jsonFactory )
                .setServiceAccountScopes( AnalyticsScopes.all() )
                .setServiceAccountId( oathServiceEmail )
                .setServiceAccountPrivateKeyFromP12File( keyFile )
                .build();

        analytics = new Analytics.Builder( httpTransport, jsonFactory, credential )
                .setApplicationName( application )
                .build();
    }

    public void close() throws IOException {
        httpTransport.shutdown();
    }

    public Analytics getAnalytics() {
        return analytics;
    }

}
