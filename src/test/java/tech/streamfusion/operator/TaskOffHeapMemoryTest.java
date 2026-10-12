package tech.streamfusion.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.streamfusion.NativeMemoryLimitException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TaskOffHeapMemoryTest {

  private long previousCapacity;

  @BeforeEach
  void savePoolConfiguration() {
    previousCapacity = TaskOffHeapMemory.capacityBytes();
  }

  @AfterEach
  void reset() {
    TaskOffHeapMemory.resetForTests(previousCapacity == 0 ? -1 : previousCapacity);
  }

  @Test
  void aggregatesOwnersAgainstOneProcessCap() {
    TaskOffHeapMemory.resetForTests(100);
    long first = TaskOffHeapMemory.registerOwner("state", "first");
    long second = TaskOffHeapMemory.registerOwner("kafka", "second");

    assertTrue(TaskOffHeapMemory.tryReserve(first, 60));
    assertFalse(TaskOffHeapMemory.tryReserve(second, 41));
    assertTrue(TaskOffHeapMemory.tryReserve(second, 40));
    assertEquals(100, TaskOffHeapMemory.reservedBytes());
    assertEquals(100, TaskOffHeapMemory.peakBytes());

    TaskOffHeapMemory.release(first, 25);
    assertEquals(75, TaskOffHeapMemory.reservedBytes());
    TaskOffHeapMemory.closeOwner(first);
    TaskOffHeapMemory.closeOwner(second);
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
  }

  @Test
  void fixedReservationRollsBackAndCloseIsIdempotent() {
    TaskOffHeapMemory.resetForTests(32);
    TaskOffHeapMemory.Reservation reservation =
        TaskOffHeapMemory.reserve("kafka", "queue", 32);
    assertThrows(
        NativeMemoryLimitException.class,
        () -> TaskOffHeapMemory.reserve("kafka", "another", 1));

    reservation.close();
    reservation.close();
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
  }

  @Test
  void resettingTestConfigurationCannotHideLiveReservations() {
    TaskOffHeapMemory.resetForTests(32);
    try (TaskOffHeapMemory.Reservation reservation =
        TaskOffHeapMemory.reserve("test", "live reservation", 16)) {
      assertThrows(IllegalStateException.class, () -> TaskOffHeapMemory.resetForTests(64));
      assertEquals(32, TaskOffHeapMemory.capacityBytes());
      assertEquals(16, TaskOffHeapMemory.reservedBytes());
    }
    assertEquals(0, TaskOffHeapMemory.reservedBytes());
  }
}
