package org.apache.hop.core.exception;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Unit test for {@link HopRuntimeException} */
class HopRuntimeExceptionTests {

  @Test
  void testNoArgsConstructor() {
    HopRuntimeException ex = new HopRuntimeException();

    assertNull(ex.getMessage());
    assertNull(ex.getCause());
  }

  @Test
  void testMessageConstructor() {
    HopRuntimeException ex = new HopRuntimeException("error");

    assertEquals("error", ex.getMessage());
    assertNull(ex.getCause());
  }

  @Test
  void testCauseConstructor() {
    Throwable cause = new RuntimeException("root cause");
    HopRuntimeException ex = new HopRuntimeException(cause);

    assertEquals(cause, ex.getCause());
    assertTrue(ex.getMessage().contains("root cause"));
  }

  @Test
  void testMessageAndCauseConstructor() {
    Throwable cause = new RuntimeException("root cause");
    HopRuntimeException ex = new HopRuntimeException("error", cause);

    assertEquals("error", ex.getMessage());
    assertEquals(cause, ex.getCause());
  }

  @Test
  void testStringAndExceptionConstructor() {
    Exception cause = new Exception("checked exception");
    HopRuntimeException ex = new HopRuntimeException("error", cause);

    assertEquals("error", ex.getMessage());
    assertEquals(cause, ex.getCause());
  }
}
