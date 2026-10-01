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

package org.apache.hop.pipeline.transforms.jms.shared;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;

/**
 * A reusable JMS broker connection, shared by the JMS consumer and producer transforms.
 *
 * <p>Two ways to reach a broker:
 *
 * <ul>
 *   <li><b>Direct</b> — a broker URL handed to the bundled Apache ActiveMQ Artemis client. Nothing
 *       else to install.
 *   <li><b>JNDI</b> — an initial context factory and a connection factory name. This is how any
 *       other JMS 3.0 provider is used: put its client jar beside this plugin and name its factory
 *       here.
 * </ul>
 */
@Getter
@Setter
@HopMetadata(
    key = "jms-connection",
    name = "i18n::JmsConnection.Name",
    description = "i18n::JmsConnection.Description",
    image = "jms-connection.svg",
    category = HopMetadataCategory.CONNECTIONS,
    documentationUrl = "/metadata-types/jms-connection.html")
public class JmsConnection extends HopMetadataBase implements Serializable, IHopMetadata {

  private static final long serialVersionUID = 1L;

  public static final String MODE_DIRECT = "DIRECT";
  public static final String MODE_JNDI = "JNDI";

  /** {@link #MODE_DIRECT} or {@link #MODE_JNDI}. */
  @HopMetadataProperty(key = "mode")
  private String mode = MODE_DIRECT;

  /** Broker URL for DIRECT mode, e.g. {@code tcp://localhost:61616}. Supports variables. */
  @HopMetadataProperty(key = "broker_url")
  private String brokerUrl;

  /** JNDI initial context factory class name for JNDI mode. Supports variables. */
  @HopMetadataProperty(key = "initial_context_factory")
  private String initialContextFactory;

  /** JNDI provider URL for JNDI mode. Supports variables. */
  @HopMetadataProperty(key = "provider_url")
  private String providerUrl;

  /** Name the connection factory is bound to in JNDI. Supports variables. */
  @HopMetadataProperty(key = "connection_factory_name")
  private String connectionFactoryName = "ConnectionFactory";

  @HopMetadataProperty(key = "username")
  private String username;

  @HopMetadataProperty(key = "password", password = true)
  private String password;

  /** Optional JMS client id. Required by the spec for a durable topic subscription. */
  @HopMetadataProperty(key = "client_id")
  private String clientId;

  public JmsConnection() {
    super();
  }

  public JmsConnection(String name) {
    super(name);
  }

  public boolean isJndi() {
    return MODE_JNDI.equalsIgnoreCase(mode);
  }
}
