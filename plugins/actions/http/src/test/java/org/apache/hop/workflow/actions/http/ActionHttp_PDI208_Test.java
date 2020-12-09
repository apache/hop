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

package org.apache.hop.workflow.actions.http;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.io.FileUtils;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

public class ActionHttp_PDI208_Test {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  public static final String HTTP_HOST = "localhost";
  public static final int HTTP_PORT = 9998;
  public static final String HTTP_SERVER_BASEURL = "http://localhost:9998";

  private static HttpServer httpServer;

  @BeforeClass
  public static void setupBeforeClass() throws HopException, IOException {
    HopClientEnvironment.init();
    ActionHttp_PDI208_Test.startHttpServer();
  }

  @AfterClass
  public static void tearDown() {
    ActionHttp_PDI208_Test.stopHttpServer();
  }

  @Test
  public void testHttpResultDefaultRows() throws IOException {
    File localFileForUpload = getInputFile( "existingFile1", ".tmp" );
    File tempFileForDownload = File.createTempFile( "downloadedFile1", ".tmp" );
    localFileForUpload.deleteOnExit();
    tempFileForDownload.deleteOnExit();

    Object[] r = new Object[] { HTTP_SERVER_BASEURL + "/uploadFile",
      localFileForUpload.getCanonicalPath(), tempFileForDownload.getCanonicalPath() };
    RowMeta rowMetaDefault = new RowMeta();
    rowMetaDefault.addValueMeta( new ValueMetaString( "URL" ) );
    rowMetaDefault.addValueMeta( new ValueMetaString( "UPLOAD" ) );
    rowMetaDefault.addValueMeta( new ValueMetaString( "DESTINATION" ) );
    List<RowMetaAndData> rows = new ArrayList<>();
    rows.add( new RowMetaAndData( rowMetaDefault, r ) );
    Result previousResult = new Result();
    previousResult.setRows( rows );

    ActionHttp http = new ActionHttp();
    http.setParentWorkflow( new LocalWorkflowEngine() );
    http.setRunForEveryRow( true );
    http.setAddFilenameToResult( false );
    http.execute( previousResult, 0 );
    assertTrue( FileUtils.contentEquals( localFileForUpload, tempFileForDownload ) );
  }

  @Test
  public void testHttpResultCustomRows() throws IOException {
    File localFileForUpload = getInputFile( "existingFile2", ".tmp" );
    File tempFileForDownload = File.createTempFile( "downloadedFile2", ".tmp" );
    localFileForUpload.deleteOnExit();
    tempFileForDownload.deleteOnExit();

    Object[] r = new Object[] { HTTP_SERVER_BASEURL + "/uploadFile",
      localFileForUpload.getCanonicalPath(), tempFileForDownload.getCanonicalPath() };
    RowMeta rowMetaDefault = new RowMeta();
    rowMetaDefault.addValueMeta( new ValueMetaString( "MyURL" ) );
    rowMetaDefault.addValueMeta( new ValueMetaString( "MyUpload" ) );
    rowMetaDefault.addValueMeta( new ValueMetaString( "MyDestination" ) );
    List<RowMetaAndData> rows = new ArrayList<>();
    rows.add( new RowMetaAndData( rowMetaDefault, r ) );
    Result previousResult = new Result();
    previousResult.setRows( rows );

    ActionHttp http = new ActionHttp();
    http.setParentWorkflow( new LocalWorkflowEngine() );
    http.setRunForEveryRow( true );
    http.setAddFilenameToResult( false );
    http.setUrlFieldname( "MyURL" );
    http.setUploadFieldname( "MyUpload" );
    http.setDestinationFieldname( "MyDestination" );
    http.execute( previousResult, 0 );
    assertTrue( FileUtils.contentEquals( localFileForUpload, tempFileForDownload ) );
  }

  private File getInputFile( String prefix, String suffix ) throws IOException {
    File inputFile = File.createTempFile( prefix, suffix );
    FileUtils.writeStringToFile( inputFile, UUID.randomUUID().toString(), "UTF-8" );
    return inputFile;
  }

  private static void startHttpServer() throws IOException {
    httpServer = HttpServer.create( new InetSocketAddress( ActionHttp_PDI208_Test.HTTP_HOST, ActionHttp_PDI208_Test.HTTP_PORT ), 10 );
    httpServer.createContext( "/uploadFile", httpExchange -> {
      Headers h = httpExchange.getResponseHeaders();
      h.add( "Content-Type", "application/octet-stream" );
      httpExchange.sendResponseHeaders( 200, 0 );
      InputStream is = httpExchange.getRequestBody();
      OutputStream os = httpExchange.getResponseBody();
      int inputChar = -1;
      while ( ( inputChar = is.read() ) >= 0 ) {
        os.write( inputChar );
      }
      is.close();
      os.flush();
      os.close();
      httpExchange.close();
    } );
    httpServer.start();
  }

  private static void stopHttpServer() {
    httpServer.stop( 2 );
  }
}
