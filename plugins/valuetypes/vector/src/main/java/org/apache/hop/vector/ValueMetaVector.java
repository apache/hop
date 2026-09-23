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

package org.apache.hop.vector;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopEofException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaPlugin;

/**
 * A dense floating point vector, as produced by an embedding model and consumed by a vector store.
 *
 * <p>The canonical text form is a bracketed, comma separated list of numbers: {@code
 * [0.1,0.2,0.3]}. That form is both valid JSON and the literal syntax pgvector accepts, so a vector
 * survives a round trip through a text file, a Data Grid, a JSON document or a database column
 * without a conversion step in between.
 *
 * <p>The field's {@code length} carries the number of dimensions, the way it carries the size of a
 * String. A length of zero or less means the dimension is unspecified. Hop never pads or truncates
 * a vector to fit a declared length: an embedding resized to match would still retrieve results,
 * and they would be wrong.
 */
@ValueMetaPlugin(
    id = "1536", // the dimension of OpenAI's text-embedding-3-small
    name = "Vector",
    description = "Dense floating point vector (embedding)",
    image = "vector.svg")
public class ValueMetaVector extends ValueMetaBase {

  public static final int TYPE_VECTOR = IValueMeta.TYPE_VECTOR;

  public ValueMetaVector() {
    super(null, TYPE_VECTOR);
  }

  public ValueMetaVector(String name) {
    super(name, TYPE_VECTOR);
  }

  public ValueMetaVector(ValueMetaVector meta) {
    super(meta.name, TYPE_VECTOR);
  }

  @Override
  public ValueMetaVector clone() {
    return (ValueMetaVector) super.clone();
  }

  @Override
  public Class<?> getNativeDataTypeClass() {
    return float[].class;
  }

  @Override
  public Object convertData(IValueMeta meta2, Object data2) throws HopValueException {
    return toVector(meta2, data2);
  }

  /**
   * Convert the specified data to a vector. Used internally instead of convertData() to avoid
   * upcasts and casts.
   */
  private float[] toVector(IValueMeta meta2, Object data2) throws HopValueException {
    if (data2 == null) {
      return null;
    }
    // Already a vector? Done.
    if (data2 instanceof float[] vector) {
      return vector;
    }
    try {
      switch (meta2.getType()) {
        case TYPE_VECTOR:
          switch (meta2.getStorageType()) {
            case STORAGE_TYPE_NORMAL:
              // Only reached when the storage type is normal and the data is still a String.
              // A float[] returns above.
              return parse((String) data2);
            case STORAGE_TYPE_BINARY_STRING:
              // The Vector field's own lazy bytes: meta2 owns the storage metadata needed to
              // decode them, so the conversion has to run on meta2 and not on this.
              return (float[]) meta2.convertBinaryStringToNativeType((byte[]) data2);
            case STORAGE_TYPE_INDEXED:
              return toVector(this, meta2.getIndex()[(Integer) data2]);
            default:
              break;
          }
          break;
        case TYPE_STRING:
          // getString resolves normal, lazily converted and indexed storage against meta2's own
          // metadata. Converting through this Vector instead would use a storage metadata that a
          // freshly built target field does not have.
          return parse(meta2.getString(data2));
        default:
          break;
      }
    } catch (HopValueException e) {
      throw e;
    } catch (ClassCastException e) {
      throw new HopValueException(
          this + " : I can't convert the specified value to data type : Vector", e);
    }
    throw new HopValueException(
        this + " : I can't convert the specified value to data type : Vector");
  }

  /**
   * Parse the canonical text form. Surrounding brackets are optional and whitespace is ignored, so
   * both {@code [0.1, 0.2]} and {@code 0.1,0.2} are accepted.
   *
   * @param string the text to parse, or null
   * @return the parsed vector, or null when the input is null or blank
   * @throws HopValueException when the text is not a list of numbers
   */
  public static float[] parse(String string) throws HopValueException {
    if (string == null) {
      return null;
    }
    String trimmed = string.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
      trimmed = trimmed.substring(1, trimmed.length() - 1).trim();
    }
    if (trimmed.isEmpty()) {
      return new float[0];
    }
    String[] parts = trimmed.split(",", -1);
    float[] vector = new float[parts.length];
    for (int i = 0; i < parts.length; i++) {
      String part = parts[i].trim();
      if (part.isEmpty()) {
        throw new HopValueException(
            "Empty element at position " + i + " while parsing a vector from '" + string + "'");
      }
      try {
        vector[i] = Float.parseFloat(part);
      } catch (NumberFormatException e) {
        throw new HopValueException(
            "'" + part + "' at position " + i + " is not a number in vector '" + string + "'", e);
      }
    }
    return vector;
  }

  /**
   * Render a vector in the canonical text form.
   *
   * @param vector the vector, or null
   * @return the text form, or null when the vector is null
   */
  public static String render(float[] vector) {
    if (vector == null) {
      return null;
    }
    StringBuilder builder = new StringBuilder(vector.length * 12 + 2);
    builder.append('[');
    for (int i = 0; i < vector.length; i++) {
      if (i > 0) {
        builder.append(',');
      }
      builder.append(vector[i]);
    }
    builder.append(']');
    return builder.toString();
  }

  @Override
  public int hashCode(Object object) throws HopValueException {
    float[] vector = toVector(this, object);
    return vector == null ? 0 : Arrays.hashCode(vector);
  }

  @Override
  public Object cloneValueData(Object object) throws HopValueException {
    if (object == null) {
      return null;
    }
    // Lazily converted values stay in their binary form, as ValueMetaBase does: materialising
    // them here would leave a float[] behind a storage type that still says binary string.
    if (storageType != STORAGE_TYPE_NORMAL) {
      return object;
    }
    // Unlike the scalar types, a vector is mutable: hand out a copy so that two rows sharing a
    // value can not write through each other.
    if (object instanceof float[] vector) {
      return vector.clone();
    }
    return toVector(this, object);
  }

  /**
   * Vectors have no meaningful natural order, but sorting, grouping and distinct all need a total
   * order that is stable. This is the lexicographic order of {@link Arrays#compare(float[],
   * float[])}: the first differing element decides, and a vector that is a prefix of another sorts
   * first.
   */
  @Override
  protected int typeCompare(Object object1, Object object2) throws HopValueException {
    float[] vector1 = toVector(this, object1);
    float[] vector2 = toVector(this, object2);
    if (vector1 == null && vector2 == null) {
      return 0;
    }
    if (vector1 == null) {
      return -1;
    }
    if (vector2 == null) {
      return 1;
    }
    return Arrays.compare(vector1, vector2);
  }

  private static float[] toFloats(double[] doubles) {
    float[] vector = new float[doubles.length];
    for (int i = 0; i < doubles.length; i++) {
      vector[i] = (float) doubles[i];
    }
    return vector;
  }

  private static float[] fromSqlArray(java.sql.Array array) throws HopValueException {
    try {
      Object elements = array.getArray();
      if (elements instanceof float[] floats) {
        return floats;
      }
      if (elements instanceof double[] doubles) {
        return toFloats(doubles);
      }
      if (elements instanceof Number[] numbers) {
        float[] vector = new float[numbers.length];
        for (int i = 0; i < numbers.length; i++) {
          vector[i] = numbers[i] == null ? 0f : numbers[i].floatValue();
        }
        return vector;
      }
      return parse(String.valueOf(elements));
    } catch (SQLException e) {
      throw new HopValueException("Unable to read a vector from a SQL array value", e);
    }
  }

  @Override
  public String getString(Object object) throws HopValueException {
    return render(toVector(this, object));
  }

  @Override
  public void setPreparedStatementValue(
      DatabaseMeta databaseMeta, PreparedStatement preparedStatement, int index, Object data)
      throws HopDatabaseException {
    try {
      float[] vector = toVector(this, data);
      if (vector == null) {
        preparedStatement.setNull(index, Types.VARCHAR);
        return;
      }
      // The canonical text form. pgvector accepts it for a vector column, and a database without
      // a vector type stores the same text in a character column. Handing the driver the float[]
      // instead is not portable: most drivers reject it, and the ones that do not tend to write a
      // serialized Java object into the column.
      preparedStatement.setString(index, render(vector));
    } catch (Exception e) {
      throw new HopDatabaseException(
          "Error setting vector value #"
              + index
              + " ["
              + toStringMeta()
              + "] on prepared statement",
          e);
    }
  }

  @Override
  public Object getValueFromResultSet(IDatabase iDatabase, ResultSet resultSet, int index)
      throws HopDatabaseException {
    try {
      Object object = resultSet.getObject(index + 1);
      if (object == null) {
        return null;
      }
      if (object instanceof float[] vector) {
        return vector;
      }
      if (object instanceof double[] doubles) {
        return toFloats(doubles);
      }
      if (object instanceof String text) {
        return parse(text);
      }
      if (object instanceof java.sql.Array array) {
        return fromSqlArray(array);
      }
      // pgvector hands back its own object type through getObject(); its toString() is the
      // canonical form, which is also what a character column returns. Anything else would give
      // Object.toString(), which parse rejects with a clearer message than a cast would.
      return parse(object.toString());
    } catch (SQLException e) {
      throw new HopDatabaseException(
          "Unable to get vector value '"
              + toStringMeta()
              + "' from database resultset, index "
              + index,
          e);
    } catch (Exception e) {
      throw new HopDatabaseException("Unable to read vector value", e);
    }
  }

  @Override
  public byte[] getBinaryString(Object object) throws HopValueException {
    if (isStorageBinaryString() && identicalFormat) {
      return (byte[]) object;
    }
    float[] vector = toVector(this, object);
    if (vector == null) {
      return null;
    }
    try {
      String encode = getStringEncoding();
      Charset charset = encode == null ? StandardCharsets.UTF_8 : Charset.forName(encode);
      return render(vector).getBytes(charset);
    } catch (Exception e) {
      throw new HopValueException("Unable to get binary string for vector", e);
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
      // Convert before writing the flag: parse maps blank text to null, so a value that
      // getString and hashCode already treat as null has to serialise as null too.
      float[] vector = toVector(this, object);
      outputStream.writeBoolean(vector == null);
      if (vector != null) {
        // Length prefixed binary floats rather than the text form: a 1536 element vector is 6 kB
        // as text and 6 kB of parsing on the way back, against 6 kB of raw floats here.
        outputStream.writeInt(vector.length);
        for (float value : vector) {
          outputStream.writeFloat(value);
        }
      }
    } catch (IOException e) {
      throw new HopFileException(this + " : Unable to write value data to output stream", e);
    } catch (Exception e) {
      throw new HopFileException(
          "Unable to convert data to a vector before writing to output stream", e);
    }
  }

  protected float[] readVector(DataInputStream inputStream) throws IOException {
    int length = inputStream.readInt();
    if (length < 0) {
      return null;
    }
    float[] vector = new float[length];
    for (int i = 0; i < length; i++) {
      vector[i] = inputStream.readFloat();
    }
    return vector;
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
      return readVector(inputStream);
    } catch (EOFException e) {
      throw new HopEofException(e);
    } catch (SocketTimeoutException e) {
      throw e;
    } catch (IOException e) {
      throw new HopFileException(this + " : Unable to read vector value data from input stream", e);
    }
  }
}
