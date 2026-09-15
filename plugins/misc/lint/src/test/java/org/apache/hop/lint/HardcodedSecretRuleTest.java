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
package org.apache.hop.lint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;

/**
 * What SEC-002 counts as a hardcoded secret.
 *
 * <p>The rule matched any field whose name merely contained "token", "secret" or "password", at
 * whatever type, so it reported Token Replacement's {@code tokenStartString} — which defaults to
 * {@code "${"} — and Get Data From XML's boolean {@code useToken} as leaked credentials, as errors,
 * on a project that had nothing wrong with it.
 *
 * @see <a href="https://github.com/apache/hop/issues/8294">#8294</a>
 */
public class HardcodedSecretRuleTest {

  /** Stands in for the stock transforms whose field names mention a secret without holding one. */
  public static class FakeTransformMeta extends BaseTransformMeta {
    private String tokenStartString = "${";
    private String tokenEndString = "}";
    private String oauth2TokenUrl = "https://example.org/oauth2/token";
    private String credentialsFile = "/etc/hop/service-account.json";
    private boolean useToken = true;
    private List<String> tokenReplacementFields = List.of("a", "b");

    private String password = "letmein";
    private String awsSessionToken = "AQoDYXdzEJr...";
    private String proxyPassword = "${PROXY_PASSWORD}";
  }

  @Test
  public void reportsOnlyTheFieldsThatActuallyHoldASecret() {
    List<String> reported = fieldsReportedFor(new FakeTransformMeta());

    assertTrue(reported.contains("password"), "a plain password is the point of the rule");
    assertTrue(reported.contains("awsSessionToken"), "a session token is a credential");
    assertEquals(2, reported.size(), "unexpected findings: " + reported);
  }

  @Test
  public void aSecretTakenFromAVariableIsNotAFinding() {
    assertTrue(
        !fieldsReportedFor(new FakeTransformMeta()).contains("proxyPassword"),
        "${PROXY_PASSWORD} is exactly what the rule asks people to do");
  }

  /** The field names that put an error on every Token Replacement and Get Data From XML step. */
  @Test
  public void aFieldNameThatMerelyMentionsASecretIsNotOne() {
    List<String> reported = fieldsReportedFor(new FakeTransformMeta());

    for (String notASecret :
        List.of(
            "tokenStartString",
            "tokenEndString",
            "oauth2TokenUrl",
            "credentialsFile",
            "useToken",
            "tokenReplacementFields")) {
      assertTrue(!reported.contains(notASecret), notASecret + " is not a credential");
    }
  }

  private static List<String> fieldsReportedFor(ITransformMeta meta) {
    CustomLintRule rule = new CustomLintRule();
    rule.setId("SEC-002");
    rule.setName("Hardcoded Password or Secret in Transform");
    rule.setSeverity("ERROR");
    rule.setEnabled(true);
    rule.setTarget(RuleTarget.TRANSFORM);
    rule.setTargetField("password");
    rule.setCondition(RuleCondition.NO_HARDCODED);

    TransformMeta transformMeta = new TransformMeta("Fake", "a transform", meta);
    List<String> fields = new ArrayList<>();
    for (LintResult result :
        CustomRuleExecutor.executeRule(rule, transformMeta, "/tmp/secrets.hpl")) {
      fields.add(fieldNameIn(result.getMessage()));
    }
    return fields;
  }

  /** The message names the field it read, between the single quotes after "in field". */
  private static String fieldNameIn(String message) {
    int start = message.indexOf("in field '");
    if (start < 0) {
      return message;
    }
    start += "in field '".length();
    return message.substring(start, message.indexOf('\'', start));
  }
}
