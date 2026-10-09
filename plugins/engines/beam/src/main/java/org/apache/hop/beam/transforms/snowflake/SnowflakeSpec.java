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

package org.apache.hop.beam.transforms.snowflake;

import java.util.regex.Pattern;
import lombok.Getter;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;

/** Resolved SnowflakeIO connection and staging settings. Secrets are not copied into errors. */
@Getter
public final class SnowflakeSpec {
  private static final Pattern SERVER =
      Pattern.compile("[A-Za-z0-9][A-Za-z0-9.-]*\\.snowflakecomputing\\.com");
  private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_$]*");
  private static final Pattern TABLE =
      Pattern.compile("[A-Za-z_][A-Za-z0-9_$]*(\\.[A-Za-z_][A-Za-z0-9_$]*){0,2}");

  private final SnowflakeIO.DataSourceConfiguration dataSource;
  private final String stagingBucket;
  private final String storageIntegration;
  private final String quotationMark;
  private final String table;
  private final String query;

  private SnowflakeSpec(
      SnowflakeIO.DataSourceConfiguration dataSource,
      String stagingBucket,
      String storageIntegration,
      String quotationMark,
      String table,
      String query) {
    this.dataSource = dataSource;
    this.stagingBucket = stagingBucket;
    this.storageIntegration = storageIntegration;
    this.quotationMark = quotationMark;
    this.table = table;
    this.query = query;
  }

  public static SnowflakeSpec build(
      IVariables variables,
      String serverName,
      String username,
      String password,
      String privateKey,
      String privateKeyPassphrase,
      String database,
      String warehouse,
      String schema,
      String role,
      String port,
      String stagingBucket,
      String storageIntegration,
      String quotationMark,
      String table,
      String query,
      boolean queryAllowed)
      throws HopException {
    String server = required(variables, serverName, "server name");
    if (!SERVER.matcher(server).matches())
      throw new HopException("Snowflake server name must look like account.snowflakecomputing.com");
    String user = required(variables, username, "user");
    String key = secret(variables, privateKey);
    String passphrase = secret(variables, privateKeyPassphrase);
    String pass = secret(variables, password);
    SnowflakeIO.DataSourceConfiguration dataSource =
        SnowflakeIO.DataSourceConfiguration.create().withServerName(server);
    if (StringUtils.isNotBlank(key)) {
      dataSource =
          StringUtils.isBlank(passphrase)
              ? dataSource.withKeyPairRawAuth(user, key)
              : dataSource.withKeyPairRawAuth(user, key, passphrase);
    } else if (StringUtils.isNotBlank(pass)) {
      dataSource = dataSource.withUsernamePasswordAuth(user, pass);
    } else {
      throw new HopException("Snowflake authentication is required");
    }
    dataSource =
        withOptional(dataSource, variables, database, "database", dataSource::withDatabase);
    dataSource =
        withOptional(dataSource, variables, warehouse, "warehouse", dataSource::withWarehouse);
    dataSource = withOptional(dataSource, variables, schema, "schema", dataSource::withSchema);
    dataSource = withOptional(dataSource, variables, role, "role", dataSource::withRole);
    String portText = text(variables, port);
    if (StringUtils.isNotBlank(portText)) {
      try {
        int parsed = Integer.parseInt(portText);
        if (parsed < 1 || parsed > 65535) throw new NumberFormatException();
        dataSource = dataSource.withPortNumber(parsed);
      } catch (NumberFormatException e) {
        throw new HopException("Snowflake port must be an integer from 1 to 65535");
      }
    }
    String bucket = required(variables, stagingBucket, "staging bucket");
    if (!bucket.startsWith("gs://") || !bucket.endsWith("/") || bucket.contains(" "))
      throw new HopException("Snowflake staging bucket must be a gs:// path ending with /");
    String integration = identifier(variables, storageIntegration, "storage integration", true);
    String quote = text(variables, quotationMark);
    if (StringUtils.isNotBlank(quote)
        && (quote.length() != 1
            || quote.charAt(0) == '\\'
            || Character.isWhitespace(quote.charAt(0))))
      throw new HopException("Snowflake quotation mark must be a single character");
    String resolvedTable = text(variables, table);
    String suppliedQuery = text(variables, query);
    if (!queryAllowed && StringUtils.isNotBlank(suppliedQuery))
      throw new HopException("Snowflake output does not take a query");
    String resolvedQuery = queryAllowed ? suppliedQuery : "";
    if (queryAllowed) {
      boolean hasTable = StringUtils.isNotBlank(resolvedTable);
      boolean hasQuery = StringUtils.isNotBlank(resolvedQuery);
      if (hasTable == hasQuery)
        throw new HopException("Snowflake input needs a table or a query, and not both");
    } else if (StringUtils.isBlank(resolvedTable)) {
      throw new HopException("Snowflake table is required");
    }
    if (StringUtils.isNotBlank(resolvedTable) && !TABLE.matcher(resolvedTable).matches())
      throw new HopException("Snowflake table name must be an identifier");
    return new SnowflakeSpec(
        dataSource,
        bucket,
        integration,
        StringUtils.isBlank(quote) ? null : quote,
        StringUtils.isBlank(resolvedTable) ? null : resolvedTable,
        StringUtils.isBlank(resolvedQuery) ? null : resolvedQuery);
  }

  public static String columnName(String name) throws HopException {
    if (name == null || !IDENTIFIER.matcher(name).matches())
      throw new HopException("Snowflake field name must be an identifier");
    return name;
  }

  public static void rejectUnbounded(PCollection.IsBounded bounded) throws HopException {
    if (bounded == PCollection.IsBounded.UNBOUNDED)
      throw new HopException(
          "Beam Snowflake output copies a bounded collection through GCS. Snowpipe streaming write is not implemented");
  }

  private static SnowflakeIO.DataSourceConfiguration withOptional(
      SnowflakeIO.DataSourceConfiguration dataSource,
      IVariables variables,
      String value,
      String label,
      java.util.function.Function<String, SnowflakeIO.DataSourceConfiguration> setter)
      throws HopException {
    String resolved = identifier(variables, value, label, false);
    return resolved == null ? dataSource : setter.apply(resolved);
  }

  private static String identifier(
      IVariables variables, String value, String label, boolean required) throws HopException {
    String resolved = required ? required(variables, value, label) : text(variables, value);
    if (StringUtils.isBlank(resolved)) return null;
    if (!IDENTIFIER.matcher(resolved).matches())
      throw new HopException("Snowflake " + label + " must be an identifier");
    return resolved;
  }

  private static String required(IVariables variables, String value, String label)
      throws HopException {
    String resolved = text(variables, value);
    if (StringUtils.isBlank(resolved))
      throw new HopException("Snowflake " + label + " is required");
    return resolved;
  }

  private static String text(IVariables variables, String value) {
    return value == null ? "" : variables.resolve(value).trim();
  }

  private static String secret(IVariables variables, String value) {
    if (value == null) return "";
    return Encr.decryptPasswordOptionallyEncrypted(
        variables.resolve(Encr.decryptPasswordOptionallyEncrypted(value)));
  }
}
