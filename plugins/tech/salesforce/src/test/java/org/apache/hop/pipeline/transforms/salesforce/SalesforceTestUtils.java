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

package org.apache.hop.pipeline.transforms.salesforce;

import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.bind.XmlObject;
import com.sforce.ws.wsdl.Constants;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.util.Base64;
import java.util.Map;
import javax.xml.namespace.QName;

/** Utility class for Salesforce unit tests */
public class SalesforceTestUtils {

  /**
   * Generate a test RSA key pair for JWT testing
   *
   * @return KeyPair with 2048-bit RSA keys
   */
  public static KeyPair generateTestKeyPair() throws Exception {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
    keyGen.initialize(2048);
    return keyGen.generateKeyPair();
  }

  /**
   * Convert a PrivateKey to PKCS8 PEM format string
   *
   * @param privateKey The private key to convert
   * @return PEM-formatted string
   */
  public static String privateKeyToPem(PrivateKey privateKey) {
    byte[] encoded = privateKey.getEncoded();
    String base64 = Base64.getEncoder().encodeToString(encoded);

    StringBuilder pem = new StringBuilder();
    pem.append("-----BEGIN PRIVATE KEY-----\n");

    // Split into 64-character lines
    int index = 0;
    while (index < base64.length()) {
      int endIndex = Math.min(index + 64, base64.length());
      pem.append(base64, index, endIndex);
      pem.append("\n");
      index = endIndex;
    }

    pem.append("-----END PRIVATE KEY-----");
    return pem.toString();
  }

  /**
   * Create a mock SObject with specified fields
   *
   * @param objectType The SObject type name
   * @param fields Map of field names to values
   * @return Configured SObject
   */
  public static SObject createMockSObject(String objectType, Map<String, Object> fields) {
    SObject sObject = new SObject();
    sObject.setType(objectType);
    sObject.setName(new QName(Constants.PARTNER_SOBJECT_NS, objectType));

    for (Map.Entry<String, Object> entry : fields.entrySet()) {
      XmlObject field = new XmlObject();
      field.setName(new QName(entry.getKey()));
      field.setValue(entry.getValue());
      sObject.addField(entry.getKey(), field);
    }

    return sObject;
  }

  /**
   * Create a mock QueryResult with specified records
   *
   * @param records Array of SObjects
   * @param done Whether this is the last batch
   * @return Configured QueryResult
   */
  public static QueryResult createMockQueryResult(SObject[] records, boolean done) {
    QueryResult qr = new QueryResult();
    qr.setRecords(records);
    qr.setSize(records != null ? records.length : 0);
    qr.setDone(done);
    if (!done) {
      qr.setQueryLocator("mock-locator-" + System.currentTimeMillis());
    }
    return qr;
  }

  /**
   * Create a simple SObject with an ID field
   *
   * @param id The ID value
   * @return SObject with ID
   */
  public static SObject createSimpleSObject(String id) {
    SObject obj = new SObject();
    obj.setType("TestObject");
    obj.setId(id);
    return obj;
  }

  /**
   * Build a mock OAuth token response JSON
   *
   * @param accessToken The access token value
   * @param refreshToken The refresh token value (can be null)
   * @param instanceUrl The instance URL
   * @return JSON string
   */
  public static String buildOAuthTokenResponse(
      String accessToken, String refreshToken, String instanceUrl) {
    StringBuilder json = new StringBuilder();
    json.append("{");
    json.append("\"access_token\":\"").append(accessToken).append("\",");
    if (refreshToken != null) {
      json.append("\"refresh_token\":\"").append(refreshToken).append("\",");
    }
    json.append("\"instance_url\":\"").append(instanceUrl).append("\",");
    json.append("\"id\":\"https://login.salesforce.com/id/00D.../005...\",");
    json.append("\"token_type\":\"Bearer\",");
    json.append("\"scope\":\"api refresh_token\"");
    json.append("}");
    return json.toString();
  }

  /**
   * Build a mock OAuth error response JSON
   *
   * @param error The error code
   * @param errorDescription The error description
   * @return JSON string
   */
  public static String buildOAuthErrorResponse(String error, String errorDescription) {
    return "{\"error\":\"" + error + "\",\"error_description\":\"" + errorDescription + "\"}";
  }
}
