/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.apache.hop.databases.cassandra.driver.datastax;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transform.ITransform;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public class DriverCQLRowHandlerTest {

  @Test
  public void testNextOutputRowNoQuery() throws Exception {
    DriverKeyspace keyspace = mock(DriverKeyspace.class);
    Session session = mock(Session.class);

    DriverCQLRowHandler rowHandler = new DriverCQLRowHandler(keyspace, session, true);
    IRowMeta rowMeta = new RowMeta();

    assertNull(rowHandler.getNextOutputRow(rowMeta, null));
  }

  @Test
  public void testQueryRows() throws Exception {
    List<Object[]> rowList = new ArrayList<Object[]>();
    rowList.add(new Object[] {1L, "a", 0.2d});
    rowList.add(new Object[] {2L, "b", 42d});

    DriverKeyspace keyspace = mock(DriverKeyspace.class);
    Session session = mock(Session.class);
    ResultSet rs = mock(ResultSet.class);

    mockColumnDefinitions(rs, DataType.cint(), DataType.text(), DataType.cdouble());

    when(session.execute(anyString())).thenReturn(rs);

    Iterator<Object[]> it = rowList.iterator();
    when(rs.isExhausted())
        .then(
            invoc -> {
              return !it.hasNext();
            });
    when(rs.one())
        .then(
            invocation -> {
              Object[] rowArr = it.next();
              Row row = mock(Row.class);
              when(row.getObject(anyInt()))
                  .then(
                      invoc -> {
                        return rowArr[(int) invoc.getArguments()[0]];
                      });
              when(row.getLong(0)).thenReturn((long) rowArr[0]);
              when(row.getDouble(2)).thenReturn((double) rowArr[2]);
              return row;
            });

    DriverCQLRowHandler rowHandler = new DriverCQLRowHandler(keyspace, session, true);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("a"));
    rowMeta.addValueMeta(new ValueMetaString("b"));
    rowMeta.addValueMeta(new ValueMetaNumber("c"));

    rowHandler.newRowQuery(
        mock(ITransform.class),
        "tab",
        "select * from tab",
        null,
        null,
        mock(ILogChannel.class));

    List<Object[]> resultRows = getNextOutputRows(rowHandler, rowMeta);
    assertEquals(2, resultRows.size());
    assertEquals(2L, resultRows.get(1)[0]);
  }

  @Test
  public void testExpandCollection() throws Exception {
    List<Object[]> rowList = new ArrayList<Object[]>();
    ArrayList<Long> numList = new ArrayList<Long>();
    numList.add(1L);
    numList.add(2L);
    numList.add(3L);
    rowList.add(new Object[] {1L, numList});
    rowList.add(new Object[] {2L, new ArrayList<Long>()});
    Iterator<Object[]> it = rowList.iterator();

    DriverKeyspace keyspace = mock(DriverKeyspace.class);
    Session session = mock(Session.class);
    ResultSet rs = mock(ResultSet.class);
    when(session.execute(anyString())).thenReturn(rs);
    when(rs.isExhausted())
        .then(
            invoc -> {
              return !it.hasNext();
            });
    when(rs.one())
        .then(
            invocation -> {
              Object[] rowArr = it.next();
              Row row = mock(Row.class);
              when(row.getObject(anyInt()))
                  .then(
                      invoc -> {
                        return rowArr[(int) invoc.getArguments()[0]];
                      });
              when(row.getLong(0)).thenReturn((long) rowArr[0]);
              return row;
            });

    mockColumnDefinitions(rs, DataType.bigint(), DataType.list(DataType.bigint()));

    DriverCQLRowHandler rowHandler = new DriverCQLRowHandler(keyspace, session, true);
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaNumber("nums"));

    rowHandler.newRowQuery(
        mock(ITransform.class),
        "tab",
        "select * from tab",
        null,
        null,
        mock(ILogChannel.class));
    List<Object[]> resultRows = getNextOutputRows(rowHandler, rowMeta);
    assertEquals(4, resultRows.size());
    assertEquals(1L, resultRows.get(0)[1]);
    assertEquals(2L, resultRows.get(3)[0]);
    assertNull(resultRows.get(3)[1]);
  }

  @Test
  public void testBatchInsert() throws Exception {
    DriverKeyspace keyspace = mock(DriverKeyspace.class);
    when(keyspace.getName()).thenReturn("ks");
    Session session = mock(Session.class);
    TableMetaData familyMeta = mock(TableMetaData.class);

    ArrayList<Object[]> batch = new ArrayList<>();
    batch.add(new Object[] {1L, "a"});
    batch.add(new Object[] {2L, "b"});
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("a spaced name"));

    when(familyMeta.getTableName()).thenReturn("tab tab");
    when(familyMeta.columnExistsInSchema(anyString())).thenReturn(true);
    DriverCQLRowHandler rowHandler = new DriverCQLRowHandler(keyspace, session, true);
    rowHandler.batchInsert(rowMeta, batch, familyMeta, null, true, null);

    verify(session, times(1))
        .execute(
            argThat(
                new ArgumentMatcher<Statement>() {
                  @Override
                  public boolean matches(Object argument) {
                    Statement stmt = (Statement) argument;
                    return stmt.toString()
                        .equals(
                            "BEGIN UNLOGGED BATCH INSERT INTO ks.\"tab tab\" (id,\"a spaced name\") "
                                + "VALUES (1,'a');INSERT INTO ks.\"tab tab\" (id,\"a spaced name\") VALUES (2,'b');APPLY BATCH;");
                  }
                }));
  }

  @Test
  public void testBatchInsertIgnoreColumns() throws Exception {
    DriverKeyspace keyspace = mock(DriverKeyspace.class);
    when(keyspace.getName()).thenReturn("ks");
    Session session = mock(Session.class);
    TableMetaData familyMeta = mock(TableMetaData.class);
    when(familyMeta.getTableName()).thenReturn("tab");
    ArrayList<Object[]> batch = new ArrayList<>();
    batch.add(new Object[] {1, 1L, 2L, 3L, 4L});
    batch.add(new Object[] {2, 5L, 6L, 7L, 8L});

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("nope"));
    rowMeta.addValueMeta(new ValueMetaInteger("there1"));
    rowMeta.addValueMeta(new ValueMetaInteger("not there"));
    rowMeta.addValueMeta(new ValueMetaInteger("there2"));
    rowMeta.addValueMeta(new ValueMetaInteger("also not there"));

    when(familyMeta.columnExistsInSchema(anyString()))
        .then(
            args -> {
              return ((String) args.getArguments()[0]).startsWith("there");
            });

    DriverCQLRowHandler rowHandler = new DriverCQLRowHandler(keyspace, session, true);
    rowHandler.setUnloggedBatch(false);
    rowHandler.batchInsert(rowMeta, batch, familyMeta, "TWO", false, null);

    verify(session, times(1))
        .execute(
            argThat(
                new ArgumentMatcher<Statement>() {
                  @Override
                  public boolean matches(Object argument) {
                    Statement stmt = (Statement) argument;
                    return stmt.toString()
                            .equals(
                                "BEGIN BATCH INSERT INTO ks.tab (there1,there2) "
                                    + "VALUES (1,3);INSERT INTO ks.tab (there1,there2) VALUES (5,7);APPLY BATCH;")
                        && stmt.getConsistencyLevel().equals(ConsistencyLevel.TWO);
                  }
                }));
  }

  @Test
  public void testQueryRowsTimestamp() {
    // Use case for existing Cassandra table with a CQL Date column
    Row row = mock(Row.class);
    ColumnDefinitions cdefs = mock(ColumnDefinitions.class);
    when(row.getColumnDefinitions()).thenReturn(cdefs);
    when(cdefs.getType(0)).thenReturn(DataType.bigint());
    when(cdefs.getType(1)).thenReturn(DataType.timestamp()); // CQL timestamp
    when(cdefs.getType(2)).thenReturn(DataType.date()); // CQL date
    when(cdefs.getType(3)).thenReturn(DataType.timestamp()); // CQL timestamp
    when(row.getLong(0)).thenReturn(1L);
    when(row.getTimestamp(1)).thenReturn(new Date(1520538054000L));
    when(row.getDate(2)).thenReturn(LocalDate.fromYearMonthDay(2018, 01, 1));
    when(row.getTimestamp(3)).thenReturn(new Date(1520298371938L));
    assertEquals(1L, DriverCQLRowHandler.readValue(new ValueMetaInteger("row"), row, 0));
    assertEquals(
        new Date(1520538054000L),
        DriverCQLRowHandler.readValue(new ValueMetaDate("timestamp"), row, 1));
    assertEquals(
        new Date(1514764800000L),
        DriverCQLRowHandler.readValue(new ValueMetaDate("datestamp"), row, 2));
    assertEquals(
        new Date(1520298371938L),
        DriverCQLRowHandler.readValue(new ValueMetaDate("datestamp2"), row, 3));
  }

  protected void mockColumnDefinitions(ResultSet rs, DataType... dataTypes) {
    ColumnDefinitions cdef = mock(ColumnDefinitions.class);
    when(cdef.size()).thenReturn(dataTypes.length);
    for (int i = 0; i < dataTypes.length; i++) {
      when(cdef.getType(i)).thenReturn(dataTypes[i]);
    }
    when(rs.getColumnDefinitions()).thenReturn(cdef);
  }

  protected List<Object[]> getNextOutputRows(DriverCQLRowHandler rowHandler, IRowMeta rowMeta)
      throws Exception {
    List<Object[]> resultRows = new ArrayList<>();
    Object[][] rows = null;
    while ((rows = rowHandler.getNextOutputRow(rowMeta, null)) != null) {
      for (Object[] row : rows) {
        resultRows.add(row);
      }
    }
    return resultRows;
  }
}
