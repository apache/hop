package org.apache.hop.pipeline.transforms.ldapinput;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.AdditionalAnswers;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LdapConnectionTest {
  
  @Mock
  private ILogChannel logChannelInterface;
  
  @Mock
  private IVariables variables;
  
  private LdapInputMeta meta;
  
  @Rule
  public ExpectedException expectedEx = ExpectedException.none();
  
  @Test
  public void testLdapConnect()  {
    meta = new LdapInputMeta();
    meta.setProtocol("LDAP");
    meta.setHost("localhost");
    meta.setPort("1389");
    
    when(variables.environmentSubstitute(Matchers.<String>any())).thenAnswer(AdditionalAnswers.returnsFirstArg());
    
    LdapConnection connection;
    try {
      connection = new LdapConnection(logChannelInterface, variables, meta, null);
      connection.connect("cn=Directory Manager","password");
    } catch (HopException e) {
      fail(e.getMessage());
    }
  }
  
  @Test
  public void testLdapConnectBadCredential() throws HopException {
    meta = new LdapInputMeta();
    meta.setProtocol("LDAP");
    meta.setHost("localhost");
    meta.setPort("1389");
    
    when(variables.environmentSubstitute(Matchers.<String>any())).thenAnswer(AdditionalAnswers.returnsFirstArg());
    
    expectedEx.expect(HopException.class);
    expectedEx.expectMessage("Invalid Credentials");
    
    LdapConnection connection;
    connection = new LdapConnection(logChannelInterface, variables, meta, null);
    connection.connect("cn=Directory Manager","idontknow");
  }
  
  //Failing test case - TODO Need to mock Utils.resolvePassword
  public void testLdapsTrustAllConnect()  {
    
    meta = new LdapInputMeta();
    meta.setProtocol("LDAP SSL");
    meta.setHost("localhost");
    meta.setPort("1636");
    meta.setTrustStorePath("self-signed.truststore");
    meta.setTrustStorePassword("changeit");
    meta.setUseCertificate(true);
    meta.setTrustAllCertificates(true);
    
    when(variables.environmentSubstitute(Matchers.<String>any())).thenAnswer(AdditionalAnswers.returnsFirstArg());
    
    
    LdapConnection connection;
    try {
      connection = new LdapConnection(logChannelInterface, variables, meta, null);
      connection.connect("cn=Directory Manager","password");
    } catch (HopException e) {
      fail(e.getMessage());
    }
  }
  
  //Failing test case - I expect connection to be successful
  public void testLdapsTrustOnlyStoreConnect()  {
    
    meta = new LdapInputMeta();
    meta.setProtocol("LDAP SSL");
    meta.setHost("localhost");
    meta.setPort("1636");
    meta.setTrustStorePath(getClass().getClassLoader().getResource("self-signed.truststore").getPath());
    meta.setTrustStorePassword("changeit");
    meta.setUseCertificate(true);
    meta.setTrustAllCertificates(false);
    
    when(variables.environmentSubstitute(Matchers.<String>any())).thenAnswer(AdditionalAnswers.returnsFirstArg());
    
    LdapConnection connection;
    try {
      connection = new LdapConnection(logChannelInterface, variables, meta, null);
      connection.connect("cn=Directory Manager","password");
    } catch (HopException e) {
      fail(e.getMessage());
    }
  }

}
