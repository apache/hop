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
package org.apache.hop.vfs.s3.s3common;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.profiles.ProfileFile;

/**
 * The AWS SDK only builds an SSO profile provider when {@code software.amazon.awssdk:sso} and
 * {@code software.amazon.awssdk:ssooidc} are on the classpath. Those were the two jars the reporter
 * of issue 9321 had to copy into {@code hop/lib/core} by hand. {@code Class.forName} on {@code
 * SsoCredentialsProvider} and {@code SsoOidcClient} fails on the previous dependency set, which
 * shipped neither module.
 *
 * <p>An S3 Connection reaches that path through the AWS SDK default credential chain or a named AWS
 * profile ({@code ~/.aws/config}), not through the credentials-file option, which is loaded as
 * {@code ProfileFile.Type.CREDENTIALS} and cannot see SSO settings.
 *
 * <p>After the classpath checks, {@code resolveCredentials()} is expected to throw because this
 * fixture has no cached SSO token. That failure must be a missing or expired token, not some other
 * unexpected error.
 *
 * @see <a href="https://github.com/apache/hop/issues/9321">issue 9321</a>
 */
class AwsSsoClasspathTest {

  @TempDir Path tempDir;

  @Test
  void sessionBasedSsoProfileLoadsSsoAndSsoOidcModules() throws Exception {
    ProfileFile profileFile = sessionProfileFile();
    try (ProfileCredentialsProvider provider =
        ProfileCredentialsProvider.builder()
            .profileFile(profileFile)
            .profileName("sso-profile")
            .build()) {
      assertSsoModulesLoaded(provider);
    }
  }

  /**
   * The DEFAULT S3 auth path uses {@link DefaultCredentialsProvider}, not a lone profile provider.
   * Static access keys in the environment or system properties would be resolved first, so this
   * case is skipped when those are set; the profile-provider test above still hangs the old
   * dependency set.
   */
  @Test
  void defaultCredentialsProviderLoadsSsoAndSsoOidcModules() throws Exception {
    assumeTrue(
        isBlank(System.getenv("AWS_ACCESS_KEY_ID"))
            && isBlank(System.getenv("AWS_SECRET_ACCESS_KEY"))
            && isBlank(System.getProperty("aws.accessKeyId"))
            && isBlank(System.getProperty("aws.secretAccessKey")),
        "Static AWS keys in the environment would hide the SSO profile step");

    ProfileFile profileFile = sessionProfileFile();
    try (DefaultCredentialsProvider provider =
        DefaultCredentialsProvider.builder()
            .profileFile(profileFile)
            .profileName("sso-profile")
            .build()) {
      assertSsoModulesLoaded(provider);
    }
  }

  private ProfileFile sessionProfileFile() throws Exception {
    Path config = tempDir.resolve("config");
    Files.writeString(
        config,
        "[profile sso-profile]\n"
            + "sso_session = my-sso\n"
            + "sso_account_id = 123456789012\n"
            + "sso_role_name = MyRole\n"
            + "\n"
            + "[sso-session my-sso]\n"
            + "sso_region = us-east-1\n"
            + "sso_start_url = https://example.awsapps.com/start\n",
        StandardCharsets.UTF_8);
    return ProfileFile.builder().content(config).type(ProfileFile.Type.CONFIGURATION).build();
  }

  private static void assertSsoModulesLoaded(AwsCredentialsProvider provider) {
    // These two classes live only in software.amazon.awssdk:sso and :ssooidc. A resolveCredentials
    // check alone is not enough: DefaultCredentialsProvider wraps a missing ssooidc module into a
    // chain error that already talks about an expired SSO session, so it would pass without them.
    assertDoesNotThrow(
        () -> Class.forName("software.amazon.awssdk.services.sso.auth.SsoCredentialsProvider"));
    assertDoesNotThrow(
        () -> Class.forName("software.amazon.awssdk.services.ssooidc.SsoOidcClient"));

    RuntimeException error = assertThrows(RuntimeException.class, provider::resolveCredentials);
    String text = flatten(error);
    assertTrue(
        text.toLowerCase().contains("token") || text.toLowerCase().contains("sso session"),
        "Expected a missing or expired SSO token once both modules had loaded, got: " + text);
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  private static String flatten(Throwable error) {
    StringBuilder text = new StringBuilder();
    for (Throwable current = error; current != null; current = current.getCause()) {
      if (current.getMessage() != null) {
        text.append(current.getMessage()).append('\n');
      }
      text.append(current.getClass().getName()).append('\n');
    }
    return text.toString();
  }
}
