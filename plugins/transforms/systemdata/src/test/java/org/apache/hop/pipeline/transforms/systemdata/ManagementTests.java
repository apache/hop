package org.apache.hop.pipeline.transforms.systemdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.management.ThreadMXBean;
import org.junit.jupiter.api.Test;

/** Unit test for {@link Management} */
class ManagementTests {

  @Test
  void testGetPID() {
    long pid = Management.getPID();

    assertTrue(pid > 0, "PID should be positive");
  }

  @Test
  void testGetJVMCpuTime() {
    long cpuTime = Management.getJVMCpuTime();

    assertTrue(cpuTime >= 0, "CPU time should be >= 0");
  }

  @Test
  void testMemoryMethods() {
    assertTrue(Management.getFreePhysicalMemorySize() >= 0);
    assertTrue(Management.getFreeSwapSpaceSize() >= 0);
    assertTrue(Management.getTotalPhysicalMemorySize() > 0);
    assertTrue(Management.getTotalSwapSpaceSize() >= 0);
    assertTrue(Management.getCommittedVirtualMemorySize() >= 0);
  }

  @Test
  void testGetCpuTime() {
    long threadId = Thread.currentThread().threadId();

    long cpuTime = Management.getCpuTime(threadId);
    assertTrue(cpuTime >= 0);
  }

  @Test
  void testGetCpuTime_notSupported() throws Exception {
    ThreadMXBean mockBean = mock(ThreadMXBean.class);
    when(mockBean.isThreadCpuTimeSupported()).thenReturn(false);

    var field = Management.class.getDeclaredField("threadBean");
    field.setAccessible(true);
    field.set(null, mockBean);

    long result = Management.getCpuTime(1L);
    assertEquals(0L, result);
  }

  @Test
  void testBeanCaching() {
    long v1 = Management.getJVMCpuTime();
    long v2 = Management.getJVMCpuTime();

    assertTrue(v1 >= 0);
    assertTrue(v2 >= 0);
  }
}
