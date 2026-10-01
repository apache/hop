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

package org.apache.hop.core.variables.resolver.vault;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.util.Base64;

/**
 * Compact RS256 JWT with Kubernetes ServiceAccount claims, for tests only. Vault's Kubernetes auth
 * backend only accepts RSA/ECDSA JWT algorithms, even when it does not verify the signature.
 */
final class KubernetesServiceAccountJwt {

  private KubernetesServiceAccountJwt() {}

  static String create(String namespace, String name) {
    String header = b64("{\"alg\":\"RS256\",\"typ\":\"JWT\"}");
    String payload =
        b64(
            "{"
                + "\"iss\":\"kubernetes/serviceaccount\","
                + "\"kubernetes.io/serviceaccount/namespace\":\""
                + namespace
                + "\","
                + "\"kubernetes.io/serviceaccount/secret.name\":\""
                + name
                + "-token\","
                + "\"kubernetes.io/serviceaccount/service-account.name\":\""
                + name
                + "\","
                + "\"kubernetes.io/serviceaccount/service-account.uid\":\"1\","
                + "\"sub\":\"system:serviceaccount:"
                + namespace
                + ":"
                + name
                + "\""
                + "}");
    String signingInput = header + "." + payload;
    return signingInput + "." + b64(rsaSign(signingInput));
  }

  private static byte[] rsaSign(String signingInput) {
    try {
      KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
      generator.initialize(2048);
      KeyPair keyPair = generator.generateKeyPair();
      Signature signature = Signature.getInstance("SHA256withRSA");
      signature.initSign(keyPair.getPrivate());
      signature.update(signingInput.getBytes(StandardCharsets.UTF_8));
      return signature.sign();
    } catch (Exception e) {
      throw new IllegalStateException("Could not sign the test JWT", e);
    }
  }

  private static String b64(String value) {
    return b64(value.getBytes(StandardCharsets.UTF_8));
  }

  private static String b64(byte[] value) {
    return Base64.getUrlEncoder().withoutPadding().encodeToString(value);
  }
}
