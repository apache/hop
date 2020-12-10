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

package org.apache.hop.pipeline.transforms.mailinput;

/**
 * Tests for MailInputMeta class
 *
 * @author Marc Batchelor - removed useless test case, added load/save tests
 * @see MailInputMeta
 */

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.workflow.actions.getpop.MailConnectionMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class MailInputMetaTest implements IInitializer<ITransformMeta> {
  LoadSaveTester loadSaveTester;
  Class<MailInputMeta> testMetaClass = MailInputMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "conditionReceivedDate", "valueimaplist", "serverName", "userName", "password", "useSSL", "port",
        "firstMails", "retrievemails", "delete", "protocol", "firstIMAPMails", "IMAPFolder", "senderSearchTerm",
        "notTermSenderSearch", "recipientSearch", "subjectSearch", "receivedDate1", "receivedDate2",
        "notTermSubjectSearch", "notTermRecipientSearch", "notTermReceivedDateSearch", "includeSubFolders", "useProxy",
        "proxyUsername", "folderField", "dynamicFolder", "rowLimit", "useBatch", "start", "end", "batchSize",
        "stopOnError", "inputFields" );

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "inputFields",
      new ArrayLoadSaveValidator<>( new MailInputFieldLoadSaveValidator(), 5 ) );
    attrValidatorMap.put( "batchSize", new IntLoadSaveValidator( 1000 ) );
    attrValidatorMap.put( "conditionReceivedDate", new IntLoadSaveValidator( MailConnectionMeta.conditionDateCode.length ) );
    attrValidatorMap.put( "valueimaplist", new IntLoadSaveValidator( MailConnectionMeta.valueIMAPListCode.length ) );
    attrValidatorMap.put( "port", new StringIntLoadSaveValidator( 65534 ) );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( ITransformMeta someMeta ) {
    if ( someMeta instanceof MailInputMeta ) {
      ( (MailInputMeta) someMeta ).allocate( 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  public class MailInputFieldLoadSaveValidator implements IFieldLoadSaveValidator<MailInputField> {
    final Random rand = new Random();

    @Override
    public MailInputField getTestObject() {
      MailInputField rtn = new MailInputField();
      rtn.setName( UUID.randomUUID().toString() );
      rtn.setColumn( rand.nextInt( MailInputField.ColumnDesc.length ) );
      return rtn;
    }

    @Override
    public boolean validateTestObject( MailInputField testObject, Object actual ) {
      if ( !( actual instanceof MailInputField ) ) {
        return false;
      }
      MailInputField another = (MailInputField) actual;
      return new EqualsBuilder()
        .append( testObject.getName(), another.getName() )
        .append( testObject.getColumn(), another.getColumn() )
        .isEquals();
    }
  }

  public class StringIntLoadSaveValidator implements IFieldLoadSaveValidator<String> {
    final Random rand = new Random();
    int intBound;

    public StringIntLoadSaveValidator() {
      intBound = 0;
    }

    public StringIntLoadSaveValidator( int bounds ) {
      if ( bounds <= 0 ) {
        throw new IllegalArgumentException( "Bad boundary for StringIntLoadSaveValidator" );
      }
      this.intBound = bounds;
    }

    @Override
    public String getTestObject() {
      int someInt = 0;
      if ( intBound > 0 ) {
        someInt = rand.nextInt( intBound );
      } else {
        someInt = rand.nextInt();
      }
      return Integer.toString( someInt );
    }

    @Override
    public boolean validateTestObject( String testObject, Object actual ) {
      return ( actual.equals( testObject ) );
    }
  }
}
