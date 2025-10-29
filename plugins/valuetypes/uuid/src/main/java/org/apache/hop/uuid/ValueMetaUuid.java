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

package org.apache.hop.uuid;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.UUID;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopEofException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaPlugin;

@ValueMetaPlugin(
    id = "32", // the number of digits in a UUID
    name = "UUID",
    description = "Universally Unique Identifier",
    image = "uuid.svg")
public class ValueMetaUuid extends ValueMetaBase {

  public static final int TYPE_UUID = 32;

  public ValueMetaUuid() {
    super(null, TYPE_UUID);
  }

  public ValueMetaUuid(String name) {
    super(name, TYPE_UUID);
  }

  public ValueMetaUuid(ValueMetaUuid meta) {
    super(meta.name, TYPE_UUID);
  }

  @Override
  public ValueMetaUuid clone() {
    return (ValueMetaUuid) super.clone();
  }

  @Override
  public Class<?> getNativeDataTypeClass() {
    return UUID.class;
  }

  @Override
  public Object convertData(IValueMeta meta2, Object data2) throws HopValueException {
    return toUuid(meta2, data2);
  }

  /**
   * Convert the specified data to a UUID. This method is used internally instead of convertData()
   * to avoid upcasts/casts.
   */
  private UUID toUuid(IValueMeta meta2, Object data2) throws HopValueException {
    if (data2 == null) {
      return null;
    }

    // Already a UUID? Done.
    if (data2 instanceof java.util.UUID) return (UUID) data2;

    try {
      switch (meta2.getType()) {
        case TYPE_UUID:
          {
            switch (meta2.getStorageType()) {
              case STORAGE_TYPE_NORMAL:
                // This is reached only if the storage type is normal and the
                // data2 type is String.
                // if data2 type is UUID, code returns before this line
                return UUID.fromString((String) data2);
              case STORAGE_TYPE_BINARY_STRING:
                return (UUID) convertBinaryStringToNativeType((byte[]) data2);
              case STORAGE_TYPE_INDEXED:
                return (UUID) meta2.getIndex()[(Integer) data2];
              default:
                break;
            }
          }
          break;
        case TYPE_STRING:
          {
            switch (meta2.getStorageType()) {
              case STORAGE_TYPE_NORMAL:
                return UUID.fromString((String) data2);
              case STORAGE_TYPE_BINARY_STRING:

                // convertBinaryStringToNativeType will do a recursive call on convertData.
                // the convertData will output a UUID,
                // so no need of converting the String to a UUID
                return (UUID) convertBinaryStringToNativeType((byte[]) data2);
              case STORAGE_TYPE_INDEXED:
                return UUID.fromString((String) meta2.getIndex()[(Integer) data2]);
              default:
                break;
            }
          }
          break;
        default:
          break;
      }
    } catch (IllegalArgumentException ignore) {
      // Do nothing
    }
    throw new HopValueException(
        this + " : I can't convert the specified value to data type : UUID");
  }

  @Override
  public int hashCode(Object object) throws HopValueException {
    if (object instanceof java.util.UUID) return object.hashCode();

    UUID u = toUuid(this, object);
    return u == null ? 0 : u.hashCode();
  }

  @Override
  public Object cloneValueData(Object object) throws HopValueException {
    // UUIDs are immutable, cloning is unnecessary
    return object;
  }

  @Override
  protected int typeCompare(Object object1, Object object2) throws HopValueException {
    if (object1 instanceof UUID u1 && object2 instanceof UUID u2) {
      return u1.compareTo(u2);
    }

    UUID u1 = toUuid(this, object1);
    UUID u2 = toUuid(this, object2);
    // UUID implements Comparable
    return u1.compareTo(u2);
  }

  @Override
  public String getString(Object object) throws HopValueException {
    UUID u = toUuid(this, object);
    return u == null ? null : u.toString();
  }

  @Override
  public void setPreparedStatementValue(
      DatabaseMeta databaseMeta, PreparedStatement preparedStatement, int index, Object data)
      throws HopDatabaseException {
    try {
      UUID u = toUuid(this, data);
      if (u == null) {
        preparedStatement.setNull(index, Types.OTHER);
        return;
      }

      // Optimistic try: supposes the user uses uuid ONLY if the database supports it
      try {
        preparedStatement.setObject(index, u);
        return;
      } catch (Exception ignore) {
        // fall through to string fallback
      }

      // generic fallback to String
      preparedStatement.setString(index, u.toString());
    } catch (Exception e) {
      throw new HopDatabaseException(
          "Error setting UUID value #" + index + " [" + toStringMeta() + "] on prepared statement",
          e);
    }
  }

  @Override
  public Object getValueFromResultSet(IDatabase iDatabase, ResultSet resultSet, int index)
      throws HopDatabaseException {
    try {
      Object o = resultSet.getObject(index + 1);
      return convertData(this, o);
    } catch (SQLException e) {
      throw new HopDatabaseException(
          "Unable to get UUID value '"
              + toStringMeta()
              + "' from database resultset, index "
              + index,
          e);
    } catch (Exception e) {
      throw new HopDatabaseException("Unable to read UUID value", e);
    }
  }

  @Override
  public String getDatabaseColumnTypeDefinition(
      IDatabase iDatabase,
      String tk,
      String pk,
      boolean useAutoIncrement,
      boolean addFieldName,
      boolean addCr) {
    final String col = addFieldName ? getName() + " " : "";
    String def = "UUID";

    if (iDatabase.isMsSqlServerNativeVariant()) {
      def = "UNIQUEIDENTIFIER";
    }
    return col + def + (addCr ? Const.CR : "");
  }

  @Override
  public byte[] getBinaryString(Object object) throws HopValueException {
    if (isStorageBinaryString() && identicalFormat) {
      return (byte[]) object;
    }
    UUID u = toUuid(this, object);
    if (u == null) {
      return null;
    }

    try {
      return u.toString().getBytes(getStringEncoding() == null ? "UTF-8" : getStringEncoding());
    } catch (UnsupportedEncodingException e) {
      throw new HopValueException("Unsupported encoding for UUID", e);
    } catch (Exception e) {
      throw new HopValueException("Unable to get binary string for UUID", e);
    }
  }

  @Override
  public void writeData(DataOutputStream outputStream, Object object) throws HopFileException {
    // Delegate non-NORMAL cases to the base class
    if (getStorageType() != STORAGE_TYPE_NORMAL) {
      super.writeData(outputStream, object);
      return;
    }
    try {
      outputStream.writeBoolean(object == null);

      if (object != null) {
        UUID u = toUuid(this, object);
        byte[] b = getBinaryString(u);
        outputStream.writeInt(b.length);
        outputStream.write(b);
      }
    } catch (IOException e) {
      throw new HopFileException(this + " : Unable to write value data to output stream", e);
    } catch (Exception e) {
      throw new HopFileException(
          "Unable to convert data to UUID before writing to output stream", e);
    }
  }

  protected UUID readUuid(DataInputStream inputStream) throws IOException {
    int inputLength = inputStream.readInt();
    if (inputLength < 0) {
      return null;
    }

    byte[] chars = new byte[inputLength];
    inputStream.readFully(chars);
    String uuidStr = new String(chars, StandardCharsets.UTF_8);
    return UUID.fromString(uuidStr);
  }

  @Override
  public Object readData(DataInputStream inputStream)
      throws HopFileException, SocketTimeoutException {
    // Delegate non-NORMAL cases to the base class
    if (getStorageType() != STORAGE_TYPE_NORMAL) {
      return super.readData(inputStream);
    }

    try {
      if (inputStream.readBoolean()) {
        return null;
      }

      return readUuid(inputStream);

    } catch (EOFException e) {
      throw new HopEofException(e);
    } catch (SocketTimeoutException e) {
      throw e;
    } catch (IOException e) {
      throw new HopFileException(this + " : Unable to read UUID value data from input stream", e);
    }
  }
}
