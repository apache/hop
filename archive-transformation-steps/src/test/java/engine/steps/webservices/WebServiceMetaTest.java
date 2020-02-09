/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.trans.steps.webservices;

import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepMeta;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WebServiceMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  public void testLoadXml() throws Exception {
    Node node = getTestNode();
    DatabaseMeta dbMeta = mock( DatabaseMeta.class );
    IMetaStore metastore = mock( IMetaStore.class );
    WebServiceMeta webServiceMeta = new WebServiceMeta( node, metastore );
    assertEquals( "httpUser", webServiceMeta.getHttpLogin() );
    assertEquals( "tryandguess", webServiceMeta.getHttpPassword() );
    assertEquals( "http://webservices.gama-system.com/exchangerates.asmx?WSDL", webServiceMeta.getUrl() );
    assertEquals( "GetCurrentExchangeRate", webServiceMeta.getOperationName() );
    assertEquals( "opRequestName", webServiceMeta.getOperationRequestName() );
    assertEquals( "GetCurrentExchangeRateResult", webServiceMeta.getOutFieldArgumentName() );
    assertEquals( "aProxy", webServiceMeta.getProxyHost() );
    assertEquals( "4444", webServiceMeta.getProxyPort() );
    assertEquals( 1, webServiceMeta.getCallStep() );
    assertFalse( webServiceMeta.isPassingInputData() );
    assertTrue( webServiceMeta.isCompatible() );
    assertFalse( webServiceMeta.isReturningReplyAsString() );
    List<WebServiceField> fieldsIn = webServiceMeta.getFieldsIn();
    assertEquals( 3, fieldsIn.size() );
    assertWebServiceField( fieldsIn.get( 0 ), "Bank", "strBank", "string", 2 );
    assertWebServiceField( fieldsIn.get( 1 ), "ToCurrency", "strCurrency", "string", 2 );
    assertWebServiceField( fieldsIn.get( 2 ), "Rank", "intRank", "int", 5 );
    List<WebServiceField> fieldsOut = webServiceMeta.getFieldsOut();
    assertEquals( 1, fieldsOut.size() );
    assertWebServiceField(
      fieldsOut.get( 0 ), "GetCurrentExchangeRateResult", "GetCurrentExchangeRateResult", "decimal", 6 );
    WebServiceMeta clone = webServiceMeta.clone();
    assertNotSame( clone, webServiceMeta );
    assertEquals( clone.getXML(), webServiceMeta.getXML() );
  }

  void assertWebServiceField( WebServiceField webServiceField, String name, String wsName, String xsdType, int type ) {
    assertEquals( name, webServiceField.getName() );
    assertEquals( wsName, webServiceField.getWsName() );
    assertEquals( xsdType, webServiceField.getXsdType() );
    assertEquals( type, webServiceField.getType() );
  }

  @Test
  public void testGetFields() throws Exception {
    WebServiceMeta webServiceMeta = new WebServiceMeta();
    webServiceMeta.setDefault();
    RowMetaInterface rmi = mock( RowMetaInterface.class );
    RowMetaInterface rmi2 = mock( RowMetaInterface.class );
    StepMeta nextStep = mock( StepMeta.class );
    IMetaStore metastore = mock( IMetaStore.class );
    WebServiceField field1 = new WebServiceField();
    field1.setName( "field1" );
    field1.setWsName( "field1WS" );
    field1.setXsdType( "string" );
    WebServiceField field2 = new WebServiceField();
    field2.setName( "field2" );
    field2.setWsName( "field2WS" );
    field2.setXsdType( "string" );
    WebServiceField field3 = new WebServiceField();
    field3.setName( "field3" );
    field3.setWsName( "field3WS" );
    field3.setXsdType( "string" );
    webServiceMeta.setFieldsOut( Arrays.asList( field1, field2, field3 ) );
    webServiceMeta.getFields( rmi, "idk", new RowMetaInterface[] { rmi2 }, nextStep, new Variables(), metastore );
    verify( rmi ).addValueMeta( argThat( matchValueMetaString( "field1" ) ) );
    verify( rmi ).addValueMeta( argThat( matchValueMetaString( "field2" ) ) );
    verify( rmi ).addValueMeta( argThat( matchValueMetaString( "field3" ) ) );
  }

  private Matcher<ValueMetaInterface> matchValueMetaString( final String fieldName ) {
    return new BaseMatcher<ValueMetaInterface>() {
      @Override public boolean matches( Object item ) {
        return fieldName.equals( ( (ValueMetaString) item ).getName() );
      }

      @Override public void describeTo( Description description ) {

      }
    };
  }

  @Test
  public void testCheck() throws Exception {
    WebServiceMeta webServiceMeta = new WebServiceMeta();
    TransMeta transMeta = mock( TransMeta.class );
    StepMeta stepMeta = mock( StepMeta.class );
    RowMetaInterface prev = mock( RowMetaInterface.class );
    RowMetaInterface info = mock( RowMetaInterface.class );
    IMetaStore metastore = mock( IMetaStore.class );
    String[] input = { "one" };
    ArrayList<CheckResultInterface> remarks = new ArrayList<>();
    webServiceMeta.check(
      remarks, transMeta, stepMeta, null, input, null, info, new Variables(), metastore );
    assertEquals( 2, remarks.size() );
    assertEquals( "Not receiving any fields from previous steps!", remarks.get( 0 ).getText() );
    assertEquals( "Step is receiving info from other steps.", remarks.get( 1 ).getText() );

    remarks.clear();
    webServiceMeta.setInFieldArgumentName( "ifan" );
    when( prev.size() ).thenReturn( 2 );
    webServiceMeta.check(
      remarks, transMeta, stepMeta, prev, new String[] {}, null, info, new Variables(), metastore );
    assertEquals( 2, remarks.size() );
    assertEquals( "Step is connected to previous one, receiving 2 fields", remarks.get( 0 ).getText() );
    assertEquals( "No input received from other steps!", remarks.get( 1 ).getText() );
  }

  @Test
  public void testGetFieldOut() throws Exception {
    DatabaseMeta dbMeta = mock( DatabaseMeta.class );
    IMetaStore metastore = mock( IMetaStore.class );
    WebServiceMeta webServiceMeta = new WebServiceMeta( getTestNode(), metastore );
    assertNull( webServiceMeta.getFieldOutFromWsName( "", true ) );
    assertEquals(
      "GetCurrentExchangeRateResult",
      webServiceMeta.getFieldOutFromWsName( "GetCurrentExchangeRateResult", false ).getName() );
    assertEquals(
      "GetCurrentExchangeRateResult",
      webServiceMeta.getFieldOutFromWsName( "something:GetCurrentExchangeRateResult", true ).getName() );

  }

  private Node getTestNode() throws HopXMLException {
    String xml =
      "  <step>\n"
        + "    <name>Web services lookup</name>\n"
        + "    <type>WebServiceLookup</type>\n"
        + "    <description/>\n"
        + "    <distribute>Y</distribute>\n"
        + "    <custom_distribution/>\n"
        + "    <copies>1</copies>\n"
        + "         <partitioning>\n"
        + "           <method>none</method>\n"
        + "           <schema_name/>\n"
        + "           </partitioning>\n"
        + "    <wsURL>http&#x3a;&#x2f;&#x2f;webservices.gama-system.com&#x2f;exchangerates.asmx&#x3f;WSDL</wsURL>\n"
        + "    <wsOperation>GetCurrentExchangeRate</wsOperation>\n"
        + "    <wsOperationRequest>opRequestName</wsOperationRequest>\n"
        + "    <wsOperationNamespace>http&#x3a;&#x2f;&#x2f;www.gama-system.com&#x2f;"
        + "webservices</wsOperationNamespace>\n"
        + "    <wsInFieldContainer/>\n"
        + "    <wsInFieldArgument/>\n"
        + "    <wsOutFieldContainer>GetCurrentExchangeRateResult</wsOutFieldContainer>\n"
        + "    <wsOutFieldArgument>GetCurrentExchangeRateResult</wsOutFieldArgument>\n"
        + "    <proxyHost>aProxy</proxyHost>\n"
        + "    <proxyPort>4444</proxyPort>\n"
        + "    <httpLogin>httpUser</httpLogin>\n"
        + "    <httpPassword>tryandguess</httpPassword>\n"
        + "    <callStep>1</callStep>\n"
        + "    <passingInputData>N</passingInputData>\n"
        + "    <compatible>Y</compatible>\n"
        + "    <repeating_element/>\n"
        + "    <reply_as_string>N</reply_as_string>\n"
        + "    <fieldsIn>\n"
        + "    <field>\n"
        + "        <name>Bank</name>\n"
        + "        <wsName>strBank</wsName>\n"
        + "        <xsdType>string</xsdType>\n"
        + "    </field>\n"
        + "    <field>\n"
        + "        <name>ToCurrency</name>\n"
        + "        <wsName>strCurrency</wsName>\n"
        + "        <xsdType>string</xsdType>\n"
        + "    </field>\n"
        + "    <field>\n"
        + "        <name>Rank</name>\n"
        + "        <wsName>intRank</wsName>\n"
        + "        <xsdType>int</xsdType>\n"
        + "    </field>\n"
        + "      </fieldsIn>\n"
        + "    <fieldsOut>\n"
        + "    <field>\n"
        + "        <name>GetCurrentExchangeRateResult</name>\n"
        + "        <wsName>GetCurrentExchangeRateResult</wsName>\n"
        + "        <xsdType>decimal</xsdType>\n"
        + "    </field>\n"
        + "      </fieldsOut>\n"
        + "     <cluster_schema/>\n"
        + " <remotesteps>   <input>   </input>   <output>   </output> </remotesteps>    <GUI>\n"
        + "      <xloc>331</xloc>\n"
        + "      <yloc>207</yloc>\n"
        + "      <draw>Y</draw>\n"
        + "      </GUI>\n"
        + "    </step>\n";
    return XMLHandler.loadXMLString( xml, "step" );
  }


}
