/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.apache.hop.pipeline.transforms.ldapinput;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.InitializerInterface;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.FieldLoadSaveValidator;
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

public class LDAPInputMetaTest implements InitializerInterface<ITransform> {
  LoadSaveTester loadSaveTester;
  Class<LDAPInputMeta> testMetaClass = LDAPInputMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "useAuthentication", "paging", "pageSize", "includeRowNumber", "rowNumberField", "rowLimit", "Host", "userName",
        "password", "port", "filterString", "searchBase", "timeLimit", "multiValuedSeparator", "dynamicSearch", "dynamicSearchFieldName",
        "dynamicFilter", "dynamicFilterFieldName", "searchScope", "protocol", "useCertificate", "trustStorePath", "trustStorePassword",
        "trustAllCertificates", "inputFields" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "useAuthentication", "UseAuthentication" );
        put( "includeRowNumber", "includeRowNumber" );
      }
    };

    Map<String, String> setterMap = new HashMap<>();

    Map<String, FieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();
    attrValidatorMap.put( "inputFields",
      new ArrayLoadSaveValidator<LDAPInputField>( new LDAPInputFieldLoadSaveValidator(), 5 ) );
    attrValidatorMap.put( "searchScope", new IntLoadSaveValidator( LDAPInputMeta.searchScopeCode.length ) );

    Map<String, FieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( ITransform someMeta ) {
    if ( someMeta instanceof LDAPInputMeta ) {
      ( (LDAPInputMeta) someMeta ).allocate( 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }


  public class LDAPInputFieldLoadSaveValidator implements FieldLoadSaveValidator<LDAPInputField> {
    final Random rand = new Random();

    @Override
    public LDAPInputField getTestObject() {
      LDAPInputField rtn = new LDAPInputField();
      rtn.setCurrencySymbol( UUID.randomUUID().toString() );
      rtn.setDecimalSymbol( UUID.randomUUID().toString() );
      rtn.setFormat( UUID.randomUUID().toString() );
      rtn.setGroupSymbol( UUID.randomUUID().toString() );
      rtn.setName( UUID.randomUUID().toString() );
      rtn.setTrimType( rand.nextInt( 4 ) );
      rtn.setPrecision( rand.nextInt( 9 ) );
      rtn.setRepeated( rand.nextBoolean() );
      rtn.setLength( rand.nextInt( 50 ) );
      rtn.setType( rand.nextInt( 7 ) );
      rtn.setSortedKey( rand.nextBoolean() );
      rtn.setFetchAttributeAs( rand.nextInt( LDAPInputField.FetchAttributeAsCode.length ) );
      rtn.setAttribute( UUID.randomUUID().toString() );
      return rtn;
    }

    @Override
    public boolean validateTestObject( LDAPInputField testObject, Object actual ) {
      if ( !( actual instanceof LDAPInputField ) ) {
        return false;
      }
      LDAPInputField another = (LDAPInputField) actual;
      return new EqualsBuilder()
        .append( testObject.getName(), another.getName() )
        .append( testObject.getAttribute(), another.getAttribute() )
        .append( testObject.getReturnType(), another.getReturnType() )
        .append( testObject.getType(), another.getType() )
        .append( testObject.getLength(), another.getLength() )
        .append( testObject.getFormat(), another.getFormat() )
        .append( testObject.getTrimType(), another.getTrimType() )
        .append( testObject.getPrecision(), another.getPrecision() )
        .append( testObject.getCurrencySymbol(), another.getCurrencySymbol() )
        .append( testObject.getDecimalSymbol(), another.getDecimalSymbol() )
        .append( testObject.getGroupSymbol(), another.getGroupSymbol() )
        .append( testObject.isRepeated(), another.isRepeated() )
        .append( testObject.isSortedKey(), another.isSortedKey() )
        .isEquals();
    }
  }
}
