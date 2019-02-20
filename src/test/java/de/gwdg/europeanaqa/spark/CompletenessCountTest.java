package de.gwdg.europeanaqa.spark;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CompletenessCountTest {

  @Test
  public void testOneNumber() {
    assertEquals(1, CompletenessCount.extractCores("local[1]"));
  }

  @Test
  public void testTwoNumbers() {
    assertEquals(16, CompletenessCount.extractCores("local[16]"));
  }
}
