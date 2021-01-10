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

package org.apache.hop.core;

import org.apache.hop.core.exception.HopEofException;
import org.apache.hop.core.exception.HopFileException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;

/**
 * This class represents a single entry in a database cache. A single entry in this case usually means: a single SQL
 * query.
 *
 * @author Matt
 * @since 15-01-04
 */
public class DbCacheEntry {
  private String dbname;
  private String sql;
  private int hashCode;

  public DbCacheEntry( String dbname, String sql ) {
    this.dbname = dbname;
    this.sql = sql;
  }

  public DbCacheEntry() {
    this( null, null );
  }

  public boolean sameDB( String otherDb ) {
    // ESCA-JAVA0071:
    if ( dbname == otherDb ) {
      // String comparison is actually ok here!!! This will check whether the strings
      // are really the same string object. If they're not the same String object, but they
      // contain the same value this will be catched later on in this method.
      //
      // This is supposed to be an optimization (not by me). Sven Boden

      return true; // short-circuit object equivalence, treat nulls as
      // equal
    }
    if ( null != dbname ) {
      return dbname.equalsIgnoreCase( otherDb );
    }
    return false;
  }

  @Override
  public int hashCode() {
    if ( ( 0 >= hashCode ) && ( null != dbname ) && ( null != sql ) ) {
      hashCode = dbname.toLowerCase().hashCode() ^ sql.toLowerCase().hashCode();
    }
    return hashCode;
  }

  @Override
  public boolean equals( Object o ) {
    if ( o instanceof DbCacheEntry ) {
      DbCacheEntry obj = (DbCacheEntry) o;

      return dbname.equalsIgnoreCase( obj.dbname ) && sql.equalsIgnoreCase( obj.sql );
    }
    return false;
  }

  /**
   * Read the data for this Cache entry from a data input stream
   *
   * @param dis The DataInputStream to read this entry from.
   * @throws HopFileException if the cache can't be read from disk when it should be able to. If the cache file doesn't exists, no
   *                          exception is thrown
   */
  public DbCacheEntry( DataInputStream dis ) throws HopFileException {
    try {
      dbname = dis.readUTF();
      sql = dis.readUTF();
    } catch ( EOFException eof ) {
      throw new HopEofException( "End of file reached", eof );
    } catch ( Exception e ) {
      throw new HopFileException( "Unable to read cache entry from data input stream", e );
    }
  }

  /**
   * Write the data for this Cache entry to a data output stream
   *
   * @param dos The DataOutputStream to write this entry to.
   * @return True if all went well, false if an error occured!
   */
  public boolean write( DataOutputStream dos ) {
    try {
      dos.writeUTF( dbname );
      dos.writeUTF( sql );

      return true;
    } catch ( Exception e ) {
      return false;
    }
  }
}
