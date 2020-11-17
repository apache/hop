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

package org.apache.hop.pipeline.transforms.salesforceutils;

import org.apache.hop.core.logging.ILogChannel;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Constructor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Tatsiana_Kasiankova
 *
 */
public class SalesforceUtilsTest {

  private static ILogChannel logMock;

  private String inputFieldName;

  private String expectedFieldName;

  @Before
  public void setUp() {
    logMock = mock( ILogChannel.class );
  }

  @Test
  public void testNoInstances() {
    Constructor<?>[] methods = SalesforceUtils.class.getConstructors();
    for ( int i = 0; i < methods.length; i++ ) {
      assertFalse( methods[i].isAccessible() );
    }
  }

  @Test
  public void testFieldWithExtIdYes_StandartObject() {
    inputFieldName = "Account:ExtID_AccountId__c/Account";
    expectedFieldName = "AccountId";
    String fieldToNullName = SalesforceUtils.getFieldToNullName( logMock, inputFieldName, true );
    assertEquals( expectedFieldName, fieldToNullName );
  }

  @Test
  public void testFieldWithExtIdNo_StandartObject() {
    inputFieldName = "AccountId";
    expectedFieldName = "AccountId";
    String fieldToNullName = SalesforceUtils.getFieldToNullName( logMock, inputFieldName, false );
    assertEquals( expectedFieldName, fieldToNullName );
  }

  @Test
  public void testFieldWithExtIdYes_CustomObject() {
    inputFieldName = "ParentObject__c:Name/ParentObjectId__r";
    expectedFieldName = "ParentObjectId__c";
    String fieldToNullName = SalesforceUtils.getFieldToNullName( logMock, inputFieldName, true );
    assertEquals( expectedFieldName, fieldToNullName );
  }

  @Test
  public void testFieldWithExtIdNo_CustomObject() {
    inputFieldName = "ParentObjectId__c";
    expectedFieldName = "ParentObjectId__c";
    String fieldToNullName = SalesforceUtils.getFieldToNullName( logMock, inputFieldName, false );
    assertEquals( expectedFieldName, fieldToNullName );
  }

  @Test
  public void testFieldWithExtIdYesButNameInIncorrectSyntax_StandartObject() {
    when( logMock.isDebug() ).thenReturn( true );
    inputFieldName = "Account";
    expectedFieldName = inputFieldName;
    String fieldToNullName = SalesforceUtils.getFieldToNullName( logMock, inputFieldName, true );
    assertEquals( expectedFieldName, fieldToNullName );
  }

  @Test
  public void testIncorrectExternalKeySyntaxWarnIsLoggedInDebugMode() {
    when( logMock.isDebug() ).thenReturn( true );
    inputFieldName = "AccountId";
    verify( logMock, never() ).logDebug( anyString() );
    SalesforceUtils.getFieldToNullName( logMock, inputFieldName, true );
    verify( logMock ).logDebug(
        "The field has incorrect external key syntax: AccountId. Syntax for external key should be : object:externalId/lookupField. Trying to use fieldToNullName=AccountId." );
  }

  @Test
  public void testIncorrectExternalKeySyntaxWarnIsNotLoggedInNotDebugMode() {
    when( logMock.isDebug() ).thenReturn( false );
    inputFieldName = "AccountId";
    verify( logMock, never() ).logDebug( anyString() );
    SalesforceUtils.getFieldToNullName( logMock, inputFieldName, true );
    verify( logMock, never() ).logDebug(
        "The field has incorrect external key syntax: AccountId. Syntax for external key should be : object:externalId/lookupField. Trying to use fieldToNullName=AccountId." );
  }

  @Test
  public void testFinalNullFieldNameIsLoggedInDebugMode_StandartObject() {
    when( logMock.isDebug() ).thenReturn( true );
    inputFieldName = "Account:ExtID_AccountId__c/Account";
    verify( logMock, never() ).logDebug( anyString() );
    SalesforceUtils.getFieldToNullName( logMock, inputFieldName, true );
    verify( logMock ).logDebug( "fieldToNullName=AccountId" );
  }

  @Test
  public void testFinalNullFieldNameIsLoggedInDebugMode_CustomObject() {
    when( logMock.isDebug() ).thenReturn( true );
    inputFieldName = "ParentObject__c:Name/ParentObjectId__r";
    verify( logMock, never() ).logDebug( anyString() );
    SalesforceUtils.getFieldToNullName( logMock, inputFieldName, true );
    verify( logMock ).logDebug( "fieldToNullName=ParentObjectId__c" );
  }

}
