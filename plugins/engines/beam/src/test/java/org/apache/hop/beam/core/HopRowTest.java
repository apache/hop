package org.apache.hop.beam.core;

import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HopRowTest {

  @Test
  public void equalsTest() {
    Object[] row1 = new Object[] { "AAA", "BBB", Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    Object[] row2 = new Object[] { "AAA", "BBB", Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    HopRow kettleRow1 = new HopRow(row1);
    HopRow kettleRow2 = new HopRow(row2);
    assertTrue(kettleRow1.equals( kettleRow2 ));

    Object[] row3 = new Object[] { "AAA", "BBB", Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    Object[] row4 = new Object[] { "AAA", "CCC", Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    HopRow kettleRow3 = new HopRow(row3);
    HopRow kettleRow4 = new HopRow(row4);
    assertFalse(kettleRow3.equals( kettleRow4 ));

    Object[] row5 = new Object[] { "AAA", null, Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    Object[] row6 = new Object[] { "AAA", null, Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    HopRow kettleRow5 = new HopRow(row5);
    HopRow kettleRow6 = new HopRow(row6);
    assertTrue(kettleRow5.equals( kettleRow6 ));

    Object[] row7 = new Object[] { "AAA", "BBB", Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    Object[] row8 = new Object[] { "AAA", null, Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    HopRow kettleRow7 = new HopRow(row7);
    HopRow kettleRow8 = new HopRow(row8);
    assertFalse(kettleRow7.equals( kettleRow8 ));
  }

  @Test
  public void hashCodeTest() {
    Object[] row1 = new Object[] { "AAA", "BBB", Long.valueOf( 100 ), Double.valueOf(1.234), new Date( 876876868 ) };
    HopRow kettleRow1 = new HopRow(row1);
    assertEquals( -1023250643, kettleRow1.hashCode() );
  }
}