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

package org.apache.hop.core.row;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopValueException;
import org.w3c.dom.Node;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.SocketTimeoutException;
import java.util.Date;
import java.util.List;

/**
 * This interface provides methods to describe and manipulate a row&#39;s structure. The interface is similar to the
 * JDBC ResultSet interface in that it provides a means of getting data from a row.
 * <p>
 * Inside processRow() a transform typically retrieves the structure of incoming rows by calling getInputRowMeta(), which is
 * provided by the BaseTransform class. The transform would usually clone this object and pass it to getFields() of its meta
 * class, to reflect any changes in row structure caused by the transform itself. Now the transform has IRowMeta objects
 * describing both the input and output rows.
 * <p>
 * Usually a transform would look for the indexes and types of relevant fields upon first execution of processRow(). The
 * following methods of IRowMeta are particularly useful in that regard:
 * <p>
 * <i><a href="">public int indexOfValue(...)</a></i><br/>
 * Given a field name, determine the index of the field in the row.
 * <p>
 * <i><a href="#getFieldNames()">public String[] getFieldNames()</a></i><br/>
 * Returns an array of field names. The index of a field name matches the field index in the row array.
 * <p>
 * <i><a href="#searchValueMeta(java.lang.String)">public void searchValueMeta(String valueName)</a></i><br/>
 * Given a field name, determine the meta data for the field.
 * <p>
 * <i><a href="#getValueMeta(int index)">public vValueMetaInterface getValueMeta(...)</a></i><br/>
 * Given a field index, determine the meta data for the field.<br/>
 * <p>
 * <i><a href="#getValueMetaList()">public List<IValueMeta> getValueMetaList()</a></i><br/>
 * Returns a list of all field descriptions. The index of the field description matches the field index in the row
 * array.
 * <p>
 */
public interface IRowMeta extends Cloneable {

  /**
   * Gets the value meta list.
   *
   * @return the list of value Metadata
   */
  List<IValueMeta> getValueMetaList();

  /**
   * Sets the value meta list.
   *
   * @param valueMetaList the list of valueMeta to set
   */
  void setValueMetaList( List<IValueMeta> valueMetaList );

  /**
   * Check if a value is already present in this row with the same name.
   *
   * @param meta the value to check for existence
   * @return true if a value with the same name already exists in the row
   */
  boolean exists( IValueMeta meta );

  /**
   * Add a metadata value, extends the array if needed. If a value with the same name already exists, it gets renamed.
   *
   * @param meta The metadata value to add
   */
  void addValueMeta( IValueMeta meta );

  /**
   * Add a metadata value on a certain location in the row. If a value with the same name already exists, it gets
   * renamed. Remember to change the data row according to this.
   *
   * @param index The index where the metadata value needs to be put in the row
   * @param meta  The metadata value to add to the row
   */
  void addValueMeta( int index, IValueMeta meta );

  /**
   * Get the value metadata on the specified index.
   *
   * @param index The index to get the value metadata from
   * @return The value metadata specified by the index.
   */
  IValueMeta getValueMeta( int index );

  /**
   * Replaces a value meta entry in the row metadata with another one.
   *
   * @param index     The index in the row to replace at
   * @param valueMeta the metadata to replace with
   */
  void setValueMeta( int index, IValueMeta valueMeta );

  /**
   * Get a String value from a row of data. Convert data if this needed.
   *
   * @param dataRow the data row
   * @param index   the index
   * @return The string found on that position in the row
   * @throws HopValueException in case there was a problem converting the data.
   */
  String getString( Object[] dataRow, int index ) throws HopValueException;

  /**
   * Get an Integer value from a row of data. Convert data if this needed.
   *
   * @param dataRow the data row
   * @param index   the index
   * @return The integer found on that position in the row
   * @throws HopValueException in case there was a problem converting the data.
   */
  Long getInteger( Object[] dataRow, int index ) throws HopValueException;

  /**
   * Get a Number value from a row of data. Convert data if this needed.
   *
   * @param dataRow the data row
   * @param index   the index
   * @return The number found on that position in the row
   * @throws HopValueException in case there was a problem converting the data.
   */
  Double getNumber( Object[] dataRow, int index ) throws HopValueException;

  /**
   * Get a Date value from a row of data. Convert data if this needed.
   *
   * @param dataRow the data row
   * @param index   the index
   * @return The date found on that position in the row
   * @throws HopValueException in case there was a problem converting the data.
   */
  Date getDate( Object[] dataRow, int index ) throws HopValueException;

  /**
   * Get a BigNumber value from a row of data. Convert data if this needed.
   *
   * @param dataRow the data row
   * @param index   the index
   * @return The bignumber found on that position in the row
   * @throws HopValueException in case there was a problem converting the data.
   */
  BigDecimal getBigNumber( Object[] dataRow, int index ) throws HopValueException;

  /**
   * Get a Boolean value from a row of data. Convert data if this needed.
   *
   * @param dataRow the data row
   * @param index   the index
   * @return The boolean found on that position in the row
   * @throws HopValueException in case there was a problem converting the data.
   */
  Boolean getBoolean( Object[] dataRow, int index ) throws HopValueException;

  /**
   * Get a Binary value from a row of data. Convert data if this needed.
   *
   * @param dataRow the data row
   * @param index   the index
   * @return The binary found on that position in the row
   * @throws HopValueException in case there was a problem converting the data.
   */
  byte[] getBinary( Object[] dataRow, int index ) throws HopValueException;

  /**
   * Clone row.
   *
   * @param objects objects to clone
   * @param cloneTo objects to clone to
   * @return a cloned Object[] object.
   * @throws HopValueException in case something is not quite right with the expected data
   */
  Object[] cloneRow( Object[] objects, Object[] cloneTo ) throws HopValueException;

  /**
   * Clone row.
   *
   * @param objects object to clone
   * @return a cloned objects to clone to
   * @throws HopValueException in case something is not quite right with the expected data
   */
  Object[] cloneRow( Object[] objects ) throws HopValueException;

  /**
   * Returns the size of the metadata row.
   *
   * @return the size of the metadata row
   */
  int size();

  /**
   * Returns true if there are no elements in the row metadata.
   *
   * @return true if there are no elements in the row metadata
   */
  boolean isEmpty();

  /**
   * Determines whether a value in a row is null. A value is null when the object is null. As such, you can just as good
   * write dataRow[index]==null in your code.
   *
   * @param dataRow The row of data
   * @param index   the index to reference
   * @return true if the value on the index is null.
   * @throws HopValueException in case there is a conversion error (only thrown in case of lazy conversion)
   */
  boolean isNull( Object[] dataRow, int index ) throws HopValueException;

  /**
   * Clone this IRowMeta object.
   *
   * @return a copy of this IRowMeta object
   */
  IRowMeta clone();

  /**
   * This method copies the row metadata and sets all values to the specified type (usually String)
   *
   * @param targetType The target type
   * @return The cloned metadata
   * @throws HopValueException if the target type could not be loaded from the plugin registry
   */
  IRowMeta cloneToType( int targetType ) throws HopValueException;

  /**
   * Gets the string.
   *
   * @param dataRow      the data row
   * @param valueName    the value name
   * @param defaultValue the default value
   * @return the string
   * @throws HopValueException the hop value exception
   */
  String getString( Object[] dataRow, String valueName, String defaultValue ) throws HopValueException;

  /**
   * Gets the integer.
   *
   * @param dataRow      the data row
   * @param valueName    the value name
   * @param defaultValue the default value
   * @return the integer
   * @throws HopValueException the hop value exception
   */
  Long getInteger( Object[] dataRow, String valueName, Long defaultValue ) throws HopValueException;

  /**
   * Gets the date.
   *
   * @param dataRow      the data row
   * @param valueName    the value name
   * @param defaultValue the default value
   * @return the date
   * @throws HopValueException the hop value exception
   */
  Date getDate( Object[] dataRow, String valueName, Date defaultValue ) throws HopValueException;

  /**
   * Searches for a value with a certain name in the value meta list.
   *
   * @param valueName The value name to search for
   * @return The value metadata or null if nothing was found
   */
  IValueMeta searchValueMeta( String valueName );

  /**
   * Searches the index of a value meta with a given name.
   *
   * @param valueName the name of the value metadata to look for
   * @return the index or -1 in case we didn't find the value
   */
  int indexOfValue( String valueName );

  /**
   * Add a number of fields from another row (append to the end).
   *
   * @param rowMeta The row of metadata values to add
   */
  void addRowMeta( IRowMeta rowMeta );

  /**
   * Merge the values of row r to this Row. The values that are not yet in the row are added unchanged. The values that
   * are in the row are renamed to name[2], name[3], etc.
   *
   * @param r The row to be merged with this row
   */
  void mergeRowMeta( IRowMeta r );

  /**
   * Merge the values of row r to this Row. The values that are not yet in the row are added unchanged. The values that
   * are in the row are renamed to name[2], name[3], etc.
   *
   * @param r The row to be merged with this row
   */
  void mergeRowMeta( IRowMeta r, String originTransformName );

  /**
   * Get an array of the names of all the Values in the Row.
   *
   * @return an array of Strings: the names of all the Values in the Row.
   */
  String[] getFieldNames();

  /**
   * Write a serialized version of this class (Row Metadata) to the specified outputStream.
   *
   * @param outputStream the outputstream to write to
   * @throws HopFileException in case a I/O error occurs
   */
  void writeMeta( DataOutputStream outputStream ) throws HopFileException;

  /**
   * Write a serialized version of the supplied data to the outputStream (based on the metadata but not the metadata
   * itself).
   *
   * @param outputStream the outputstream to write to
   * @param data         the data to write after the metadata
   * @throws HopFileException in case a I/O error occurs
   */
  void writeData( DataOutputStream outputStream, Object[] data ) throws HopFileException;

  /**
   * De-serialize a row of data (no metadata is read) from an input stream.
   *
   * @param inputStream the inputstream to read from
   * @return a new row of data
   * @throws HopFileException       in case a I/O error occurs
   * @throws SocketTimeoutException In case there is a timeout during reading.
   */
  Object[] readData( DataInputStream inputStream ) throws HopFileException, SocketTimeoutException;

  /**
   * Clear the row metadata.
   */
  void clear();

  /**
   * Remove a value with a certain name from the row metadata.
   *
   * @param string the name of the value metadata to remove
   * @throws HopValueException in case the value couldn't be found in the row metadata
   */
  void removeValueMeta( String string ) throws HopValueException;

  /**
   * Remove a value metadata object on a certain index in the row.
   *
   * @param index the index to remove the value metadata from
   */
  void removeValueMeta( int index );

  /**
   * Get the string representation of the data in a row of data.
   *
   * @param row the row of data to convert to string
   * @return the row of data in string form
   * @throws HopValueException in case of a conversion error
   */
  String getString( Object[] row ) throws HopValueException;

  /**
   * Get an array of strings showing the name of the values in the row padded to a maximum length, followed by the types
   * of the values.
   *
   * @param maxlen The length to which the name will be padded.
   * @return an array of strings: the names and the types of the fieldnames in the row.
   */
  String[] getFieldNamesAndTypes( int maxlen );

  /**
   * Compare 2 rows with each other using certain values in the rows and also considering the specified ascending
   * clauses of the value metadata.
   *
   * @param rowData1 The first row of data
   * @param rowData2 The second row of data
   * @param fieldnrs the fields to compare on (in that order)
   * @return 0 if the rows are considered equal, -1 is data1 is smaller, 1 if data2 is smaller.
   * @throws HopValueException the hop value exception
   */
  int compare( Object[] rowData1, Object[] rowData2, int[] fieldnrs ) throws HopValueException;

  /**
   * Compare 2 rows with each other for equality using certain values in the rows and also considering the case
   * sensitivity flag.
   *
   * @param rowData1 The first row of data
   * @param rowData2 The second row of data
   * @param fieldnrs the fields to compare on (in that order)
   * @return true if the rows are considered equal, false if they are not.
   * @throws HopValueException the hop value exception
   */
  boolean equals( Object[] rowData1, Object[] rowData2, int[] fieldnrs ) throws HopValueException;

  /**
   * Compare 2 rows with each other using certain values in the rows and also considering the specified ascending
   * clauses of the value metadata.
   *
   * @param rowData1  The first row of data
   * @param rowData2  The second row of data
   * @param fieldnrs1 The indexes of the values to compare in the first row
   * @param fieldnrs2 The indexes of the values to compare with in the second row
   * @return 0 if the rows are considered equal, -1 is data1 is smaller, 1 if data2 is smaller.
   * @throws HopValueException the hop value exception
   */
  int compare( Object[] rowData1, Object[] rowData2, int[] fieldnrs1, int[] fieldnrs2 ) throws HopValueException;

  /**
   * Compare 2 rows with each other using certain values in the rows and also considering the specified ascending
   * clauses of the value metadata.
   *
   * @param rowData1  The first row of data
   * @param rowMeta2  the metadat of the second row of data
   * @param rowData2  The second row of data
   * @param fieldnrs1 The indexes of the values to compare in the first row
   * @param fieldnrs2 The indexes of the values to compare with in the second row
   * @return 0 if the rows are considered equal, -1 is data1 is smaller, 1 if data2 is smaller.
   * @throws HopValueException the hop value exception
   */
  int compare( Object[] rowData1, IRowMeta rowMeta2, Object[] rowData2, int[] fieldnrs1,
                      int[] fieldnrs2 ) throws HopValueException;

  /**
   * Compare 2 rows with each other using all values in the rows and also considering the specified ascending clauses of
   * the value metadata.
   *
   * @param rowData1 The first row of data
   * @param rowData2 The second row of data
   * @return 0 if the rows are considered equal, -1 is data1 is smaller, 1 if data2 is smaller.
   * @throws HopValueException the hop value exception
   */
  int compare( Object[] rowData1, Object[] rowData2 ) throws HopValueException;

  /**
   * Calculate a hashCode of the content (not the index) of the data specified NOTE: This method uses a simple XOR of
   * the individual hashCodes which can result in a lot of collisions for similar types of data (e.g. [A,B] == [B,A] and
   * is not suitable for normal use. It is kept to provide backward compatibility with CombinationLookup.lookupValues()
   *
   * @param rowData The data to calculate a hashCode with
   * @return the calculated hashCode
   * @throws HopValueException in case there is a data conversion error
   * @deprecated
   */
  @Deprecated
  int oldXORHashCode( Object[] rowData ) throws HopValueException;

  /**
   * Calculates a simple hashCode of all the native data objects in the supplied row. This method will return a better
   * distribution of values for rows of numbers or rows with the same values in different positions. NOTE: This method
   * performs against the native values, not the values returned by ValueMeta. This means that if you have two rows with
   * different primitive values ['2008-01-01:12:30'] and ['2008-01-01:00:00'] that use a format object to change the
   * value (as Date yyyy-MM-dd), the hashCodes will be different resulting in the two rows not being considered equal
   * via the hashCode even though compare() or equals() might consider them to be.
   *
   * @param rowData The data to calculate a hashCode with
   * @return the calculated hashCode
   * @throws HopValueException in case there is a data conversion error
   */
  int hashCode( Object[] rowData ) throws HopValueException;

  /**
   * Calculates a hashcode of the converted value of all objects in the supplied row. This method returns distinct
   * values for nulls of different data types and will return the same hashCode for different native values that have a
   * ValueMeta converting them into the same value (e.g. ['2008-01-01:12:30'] and ['2008-01-01:00:00'] as Date
   * yyyy-MM-dd)
   *
   * @param rowData The data to calculate a hashCode with
   * @return the calculated hashCode
   * @throws HopValueException in case there is a data conversion error
   */
  int convertedValuesHashCode( Object[] rowData ) throws HopValueException;

  /**
   * To string meta.
   *
   * @return a string with a description of all the metadata values of the complete row of metadata
   */
  String toStringMeta();

  /**
   * Gets the meta xml.
   *
   * @return an XML representation of the row metadata
   * @throws IOException Thrown in case there is an (Base64/GZip) encoding problem
   */
  String getMetaXml() throws IOException;

  /**
   * Gets the data xml.
   *
   * @param rowData the row of data to serialize as XML
   * @return an XML representation of the row data
   * @throws IOException Thrown in case there is an (Base64/GZip) encoding problem
   */
  String getDataXml(Object[] rowData ) throws IOException;

  /**
   * Convert an XML node into binary data using the row metadata supplied.
   *
   * @param node The data row node
   * @return a row of data de-serialized from XML
   * @throws HopException Thrown in case there is an (Base64/GZip) decoding problem
   */
  Object[] getRow( Node node ) throws HopException;

}
