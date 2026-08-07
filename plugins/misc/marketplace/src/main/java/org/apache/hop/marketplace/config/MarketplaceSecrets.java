/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.marketplace.config;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.ITwoWayPasswordEncoder;
import org.apache.hop.core.variables.Variables;

/**
 * Password handling for marketplace repository credentials.
 *
 * <p>Repository passwords used to be written to hop-config.json in clear text. They now pass
 * through Hop's two-way password encoder on the way out ({@code Encrypted 2be98afc...}) and back
 * through it on the way in, the same way Hop server and database passwords are handled.
 *
 * <p>This is obfuscation, not encryption: the encoder key is not a secret, so anyone with the
 * config file and a Hop install can recover the password. It keeps credentials from being readable
 * over a shoulder, in a screenshot or in a diff — nothing more. Point the password at a variable
 * ({@code ${MY_TOKEN}}), a variable resolver expression, or one of the {@code HOP_MARKETPLACE_*}
 * environment variables when the secret must not live in the file at all.
 */
public final class MarketplaceSecrets {

  private MarketplaceSecrets() {}

  /**
   * Used when no encoder is bound yet — {@link Encr} stays uninitialized until {@code
   * HopClientEnvironment.init()} runs, and config can be read before that. Falling back keeps the
   * value obfuscated instead of failing or silently writing clear text.
   */
  private static final ITwoWayPasswordEncoder FALLBACK = new HopTwoWayPasswordEncoder();

  /**
   * Obfuscated form for storage. Values that use variables are stored as typed: both shipped
   * encoders leave them alone so the expression survives to be resolved at use.
   */
  public static String encode(String password) {
    if (StringUtils.isEmpty(password)) {
      return password;
    }
    return encoder().encode(password, true);
  }

  /**
   * Plain form of a stored value. Anything without the encoder's prefix is returned unchanged, so
   * configs written before this was added keep working.
   */
  public static String decode(String password) {
    if (StringUtils.isEmpty(password)) {
      return password;
    }
    return encoder().decode(password, true);
  }

  /**
   * Resolve variables and variable resolver expressions in a credential. Unresolvable expressions
   * come back unchanged — that reaches the server as a bad password, which {@link
   * MarketplaceHttp#authHint} explains.
   */
  public static String resolve(String value) {
    if (StringUtils.isEmpty(value) || !value.contains("{")) {
      return value;
    }
    try {
      return Variables.getADefaultVariableSpace().resolve(value);
    } catch (Exception e) {
      // Resolvers reach out to metadata and secret managers; a broken one must not break the
      // repository. Sending the expression unresolved fails visibly with an auth hint.
      return value;
    }
  }

  private static ITwoWayPasswordEncoder encoder() {
    ITwoWayPasswordEncoder active = Encr.getEncoder();
    return active == null ? FALLBACK : active;
  }

  /** Writes the obfuscated form of a password field. */
  public static class Serializer extends JsonSerializer<String> {
    @Override
    public void serialize(String value, JsonGenerator generator, SerializerProvider provider)
        throws IOException {
      String encoded = encode(value);
      if (encoded == null) {
        generator.writeNull();
      } else {
        generator.writeString(encoded);
      }
    }
  }

  /** Reads a password field, obfuscated or (legacy) clear text. */
  public static class Deserializer extends JsonDeserializer<String> {
    @Override
    public String deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      return decode(parser.getValueAsString());
    }
  }
}
