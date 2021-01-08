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

package org.apache.hop.pipeline.transforms.abort;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.EnumLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.junit.Test;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class AbortMetaTest {

  @Test
  public void testRoundTrip() throws HopException {
    List<String> attributes = Arrays.asList( "row_threshold", "message", "always_log_rows", "abort_option" );

    Map<String, String> getterMap = new HashMap<>();
    getterMap.put( "row_threshold", "getRowThreshold" );
    getterMap.put( "message", "getMessage" );
    getterMap.put( "always_log_rows", "isAlwaysLogRows" );
    getterMap.put( "abort_option", "getAbortOption" );

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put( "row_threshold", "setRowThreshold" );
    setterMap.put( "message", "setMessage" );
    setterMap.put( "always_log_rows", "setAlwaysLogRows" );
    setterMap.put( "abort_option", "setAbortOption" );

    Map<String, IFieldLoadSaveValidator<?>> attributeValidators = Collections.emptyMap();

    Map<String, IFieldLoadSaveValidator<?>> typeValidators = new HashMap<>();
    typeValidators.put( AbortMeta.AbortOption.class.getCanonicalName(),
      new EnumLoadSaveValidator<>( AbortMeta.AbortOption.ABORT ) );

    LoadSaveTester loadSaveTester = new LoadSaveTester( AbortMeta.class, attributes, getterMap, setterMap,
      attributeValidators, typeValidators );
    loadSaveTester.testSerialization();
  }

  @Test
  public void testBackwardsCapatibilityAbortWithError() throws HopXmlException {
    IHopMetadataProvider metadataProvider = mock( IHopMetadataProvider.class );
    AbortMeta meta = new AbortMeta();

    // Abort with error
    String inputXml = "  <transform>\n"
      + "    <name>Abort</name>\n"
      + "    <type>Abort</type>\n"
      + "    <abort_with_error>Y</abort_with_error>\n"
      + "  </transform>";
    Node node = XmlHandler.loadXmlString( inputXml ).getFirstChild();
    meta.loadXml( node, metadataProvider );
    assertTrue( meta.isAbortWithError() );

    // Don't abort with error
    inputXml = "  <transform>\n"
      + "    <name>Abort</name>\n"
      + "    <type>Abort</type>\n"
      + "    <abort_with_error>N</abort_with_error>\n"
      + "  </transform>";
    node = XmlHandler.loadXmlString( inputXml ).getFirstChild();
    meta.loadXml( node, metadataProvider );
    assertTrue( meta.isAbort() );

    // Don't abort with error
    inputXml = "  <transform>\n"
      + "    <name>Abort</name>\n"
      + "    <type>Abort</type>\n"
      + "  </transform>";
    node = XmlHandler.loadXmlString( inputXml ).getFirstChild();
    meta.loadXml( node, metadataProvider );
    assertTrue( meta.isAbortWithError() );
  }
}
