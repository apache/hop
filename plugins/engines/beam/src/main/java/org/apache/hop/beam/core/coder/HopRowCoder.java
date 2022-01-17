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

package org.apache.hop.beam.core.coder;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.row.IValueMeta;

import java.io.*;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Date;

public class HopRowCoder extends AtomicCoder<HopRow> {

  @Override
  public void encode(HopRow value, OutputStream outStream) throws CoderException, IOException {

    Object[] row = value.getRow();
    ObjectOutputStream out = new ObjectOutputStream(outStream);

    // Length
    //
    if (row == null) {
      out.writeInt(-1);
      return; // all done
    } else {
      out.writeInt(row.length);
    }

    // The values
    //
    for (int i = 0; i < row.length; i++) {
      Object object = row[i];
      // Null?
      //
      out.writeBoolean(object == null);

      if (object != null) {
        // Type?
        //
        int objectType = getObjectType(object);
        out.writeInt(objectType);

        // The object itself
        //
        write(out, objectType, object);
      }
    }
    out.flush();
  }

  @Override
  public HopRow decode(InputStream inStream) throws CoderException, IOException {

    ObjectInputStream in = new ObjectInputStream(inStream);

    Object[] row = null;
    int length = in.readInt();
    if (length < 0) {
      return new HopRow(row);
    }
    row = new Object[length];
    for (int i = 0; i < length; i++) {
      // Null?
      boolean isNull = in.readBoolean();
      if (!isNull) {
        int objectType = in.readInt();
        Object object = read(in, objectType);
        row[i] = object;
      }
    }

    return new HopRow(row);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    // Sure
  }

  private void write(ObjectOutputStream out, int objectType, Object object) throws IOException {
    switch (objectType) {
      case IValueMeta.TYPE_STRING:
        {
          String string = (String) object;
          byte[] data = string.getBytes(StandardCharsets.UTF_8);
          out.writeInt(data.length);
          out.write(data);
        }
        break;
      case IValueMeta.TYPE_INTEGER:
        {
          Long lng = (Long) object;
          out.writeLong(lng);
        }
        break;
      case IValueMeta.TYPE_TIMESTAMP:
        {
          out.writeLong(((Timestamp) object).getTime());
          out.writeInt(((Timestamp) object).getNanos());
        }
        break;
      case IValueMeta.TYPE_DATE:
        {
          Long lng = ((Date) object).getTime();
          out.writeLong(lng);
        }
        break;
      case IValueMeta.TYPE_BOOLEAN:
        {
          boolean b = (Boolean) object;
          out.writeBoolean(b);
        }
        break;
      case IValueMeta.TYPE_NUMBER:
        {
          Double dbl = (Double) object;
          out.writeDouble(dbl);
        }
        break;
      case IValueMeta.TYPE_BIGNUMBER:
        {
          BigDecimal bd = (BigDecimal) object;
          out.writeUTF(bd.toString());
        }
        break;
      case IValueMeta.TYPE_BINARY:
        {
          byte[] bytes = (byte[]) object;
          out.write(bytes.length);
          out.write(bytes);
        }
        break;
      case IValueMeta.TYPE_INET:
        {
          InetAddress inetAddress = (InetAddress) object;
          write(out, IValueMeta.TYPE_STRING, inetAddress.getHostName());
          out.writeInt(inetAddress.getAddress().length == 4 ? 1 : 2);
          out.write(inetAddress.getAddress());
        }
        break;
      default:
        throw new IOException(
            "Data type not supported yet: " + objectType + " - " + object.toString());
    }
  }

  private Object read(ObjectInputStream in, int objectType) throws IOException {
    switch (objectType) {
      case IValueMeta.TYPE_STRING:
        {
          int length = in.readInt();
          byte[] data = new byte[length];
          in.readFully(data);
          String string = new String(data, StandardCharsets.UTF_8);
          return string;
        }

      case IValueMeta.TYPE_INTEGER:
        {
          Long lng = in.readLong();
          return lng;
        }

      case IValueMeta.TYPE_TIMESTAMP:
        {
          Timestamp timestamp = new Timestamp(in.readLong());
          timestamp.setNanos(in.readInt());
          return timestamp;
        }

      case IValueMeta.TYPE_DATE:
        {
          Long lng = in.readLong();
          return new Date(lng);
        }

      case IValueMeta.TYPE_BOOLEAN:
        {
          boolean b = in.readBoolean();
          return b;
        }

      case IValueMeta.TYPE_NUMBER:
        {
          Double dbl = in.readDouble();
          return dbl;
        }

      case IValueMeta.TYPE_BIGNUMBER:
        {
          String bd = in.readUTF();
          return new BigDecimal(bd);
        }

      case IValueMeta.TYPE_BINARY:
      {
        byte[] bytes = new byte[in.readInt()];
        in.read(bytes);
        return bytes;
      }

      case IValueMeta.TYPE_INET:
      {
        String hostname = (String) read(in, IValueMeta.TYPE_STRING);
        byte[] addr = new byte[in.readInt() == 1 ? 4 : 16];
        in.read(addr);
        return InetAddress.getByAddress(hostname, addr);
      }
      default:
        throw new IOException("Data type not supported yet: " + objectType);
    }
  }

  private int getObjectType(Object object) throws CoderException {
    if (object instanceof String) {
      return IValueMeta.TYPE_STRING;
    }
    if (object instanceof Long) {
      return IValueMeta.TYPE_INTEGER;
    }
    if (object instanceof Timestamp) {
      return IValueMeta.TYPE_TIMESTAMP;
    }
    if (object instanceof Date) {
      return IValueMeta.TYPE_DATE;
    }
    if (object instanceof Boolean) {
      return IValueMeta.TYPE_BOOLEAN;
    }
    if (object instanceof Double) {
      return IValueMeta.TYPE_NUMBER;
    }
    if (object instanceof BigDecimal) {
      return IValueMeta.TYPE_BIGNUMBER;
    }
    if (object instanceof byte[]) {
      return IValueMeta.TYPE_BINARY;
    }
    if (object instanceof InetAddress) {
      return IValueMeta.TYPE_INET;
    }
    throw new CoderException(
        "Data type for object class " + object.getClass().getName() + " isn't supported yet");
  }
}
