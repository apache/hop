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

package org.apache.hop.beam.core.transform;

import java.io.Serializable;
import java.net.URI;
import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.io.mqtt.MqttIO;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;

/** Resolved worker connection settings. Do not add a toString that could expose credentials. */
@RequiredArgsConstructor
public class BeamMqttConnection implements Serializable {
  private final String serverUri;
  private final String topic;
  private final String clientId;
  private final String username;
  private final String encryptedPassword;

  public static BeamMqttConnection resolve(
      IVariables variables,
      String serverUri,
      String topic,
      String clientId,
      String username,
      String password,
      boolean source)
      throws HopException {
    String uri = required(variables, serverUri, "server URI");
    try {
      URI parsed = new URI(uri);
      if (!("tcp".equals(parsed.getScheme()) || "ssl".equals(parsed.getScheme()))
          || StringUtils.isBlank(parsed.getHost())
          || parsed.getUserInfo() != null
          || parsed.getQuery() != null
          || parsed.getFragment() != null
          || !StringUtils.isEmpty(parsed.getPath())
          || parsed.getPort() == 0
          || parsed.getPort() > 65535) throw new IllegalArgumentException();
    } catch (Exception e) {
      throw new HopException(
          "MQTT server URI must be tcp://host:port or ssl://host:port, without embedded credentials");
    }
    String resolvedTopic = required(variables, topic, "topic");
    if (resolvedTopic.indexOf(0) >= 0
        || (!source && (resolvedTopic.contains("#") || resolvedTopic.contains("+"))))
      throw new HopException("MQTT output topic must not contain wildcards or NUL");
    String user = variables.resolve(username);
    String secret =
        Encr.decryptPasswordOptionallyEncrypted(
            variables.resolve(Encr.decryptPasswordOptionallyEncrypted(password)));
    if (StringUtils.isNotEmpty(secret) && StringUtils.isBlank(user))
      throw new HopException("MQTT password requires a username");
    return new BeamMqttConnection(
        uri,
        resolvedTopic,
        variables.resolve(clientId),
        user,
        Encr.encryptPasswordIfNotUsingVariables(secret));
  }

  public MqttIO.ConnectionConfiguration createConfiguration() {
    var configuration = MqttIO.ConnectionConfiguration.create(serverUri, topic);
    if (StringUtils.isNotEmpty(clientId)) configuration = configuration.withClientId(clientId);
    if (StringUtils.isNotEmpty(username)) configuration = configuration.withUsername(username);
    if (StringUtils.isNotEmpty(encryptedPassword))
      configuration =
          configuration.withPassword(Encr.decryptPasswordOptionallyEncrypted(encryptedPassword));
    return configuration;
  }

  public static String required(IVariables variables, String value, String option)
      throws HopException {
    String resolved = variables.resolve(value);
    if (StringUtils.isBlank(resolved) || resolved.contains("${"))
      throw new HopException("Please specify a resolved MQTT " + option);
    return resolved;
  }

  public static String payloadType(IVariables variables, String value) throws HopException {
    String type = required(variables, value, "payload type");
    if (!"String".equalsIgnoreCase(type) && !"Binary".equalsIgnoreCase(type))
      throw new HopException("MQTT payload type must be String or Binary");
    return type;
  }

  public static long limit(IVariables variables, String value, String option) throws HopException {
    String resolved = variables.resolve(value);
    if (StringUtils.isBlank(resolved)) return 0;
    try {
      long limit = Long.parseLong(resolved);
      if (limit < 0) throw new NumberFormatException();
      return limit;
    } catch (NumberFormatException e) {
      throw new HopException("MQTT " + option + " must be a non-negative integer");
    }
  }
}
