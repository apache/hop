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
package org.apache.hop.databases.mariadb;

import com.google.common.collect.Sets;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.databases.mysql.MySqlDatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;

import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.util.Set;

@DatabaseMetaPlugin(
        type = "MARIADB",
        typeDescription = "MariaDB"
)
@GuiPlugin(id = "GUI-MariaDBDatabaseMeta")
public class MariaDBDatabaseMeta extends MySqlDatabaseMeta {
    private static final Class<?> PKG = MariaDBDatabaseMeta.class; // For Translator

    private static final Set<String> SHORT_MESSAGE_EXCEPTIONS = Sets.newHashSet("org.mariadb.jdbc.internal.stream.MaxAllowedPacketException");

    @Override
    public String getDriverClass() {
        return "org.mariadb.jdbc.Driver";
    }

    @Override
    public String getURL(String hostname, String port, String databaseName) {
        if (Utils.isEmpty(port)) {
            return "jdbc:mariadb://" + hostname + "/" + databaseName;
        } else {
            return "jdbc:mariadb://" + hostname + ":" + port + "/" + databaseName;
        }
    }

    @Override
    public boolean fullExceptionLog(Exception e) {
        Throwable cause = (e == null ? null : e.getCause());
        return !(cause != null && SHORT_MESSAGE_EXCEPTIONS.contains(cause.getClass().getName()));
    }

    /**
     * Returns the column name for a MariaDB field.
     *
     * @param dbMetaData
     * @param rsMetaData
     * @param index
     * @return The column label.
     * @throws HopDatabaseException
     */
    @Override
    public String getLegacyColumnName(DatabaseMetaData dbMetaData, ResultSetMetaData rsMetaData, int index) throws HopDatabaseException {
        if (dbMetaData == null) {
            throw new HopDatabaseException(BaseMessages.getString(PKG, "MariaDBDatabaseMeta.Exception.LegacyColumnNameNoDBMetaDataException"));
        }

        if (rsMetaData == null) {
            throw new HopDatabaseException(BaseMessages.getString(PKG, "MariaDBDatabaseMeta.Exception.LegacyColumnNameNoRSMetaDataException"));
        }

        try {
            return rsMetaData.getColumnLabel(index);
        } catch (Exception e) {
            throw new HopDatabaseException(String.format("%s: %s", BaseMessages.getString(PKG, "MariaDBDatabaseMeta.Exception.LegacyColumnNameException"), e.getMessage()), e);
        }
    }
}
