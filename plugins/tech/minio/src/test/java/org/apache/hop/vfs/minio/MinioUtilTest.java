package org.apache.hop.vfs.minio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class MinioUtilTest {

  @Test
  void testHasChangedWithNullValues() {
    assertFalse(MinioUtil.hasChanged(null, null), "Both null should return false");
  }

  @Test
  void testHasChangedWithEmptyValues() {
    assertFalse(MinioUtil.hasChanged("", ""), "Both empty should return false");
    assertFalse(MinioUtil.hasChanged("   ", "   "), "Both empty strings should return false");
  }

  @Test
  void testHasChangedWithNullToEmpty() {
    assertFalse(
        MinioUtil.hasChanged(null, ""), "Both null and empty are considered empty, so no change");
    assertFalse(
        MinioUtil.hasChanged("", null), "Both empty and null are considered empty, so no change");
  }

  @Test
  void testHasChangedWithDifferentValues() {
    assertTrue(MinioUtil.hasChanged("value1", "value2"), "Different values should return true");
    assertFalse(MinioUtil.hasChanged("value1", "value1"), "Same values should return false");
  }

  @Test
  void testIsEmptyWithNull() {
    assertTrue(MinioUtil.isEmpty(null), "Null should be empty");
  }

  @Test
  void testIsEmptyWithEmptyString() {
    assertTrue(MinioUtil.isEmpty(""), "Empty string should be empty");
    assertFalse(
        MinioUtil.isEmpty("   "),
        "Whitespace-only string is not considered empty by this implementation");
  }

  @Test
  void testIsEmptyWithValidString() {
    assertFalse(MinioUtil.isEmpty("test"), "Non-empty string should not be empty");
    assertFalse(MinioUtil.isEmpty("  test  "), "String with content should not be empty");
  }

  @Test
  void testConstants() {
    assertEquals(
        "aws.endpoint",
        MinioUtil.ENDPOINT_SYSTEM_PROPERTY,
        "ENDPOINT_SYSTEM_PROPERTY should be correct");
    assertEquals(
        "aws.endpoint",
        MinioUtil.SIGNATURE_VERSION_SYSTEM_PROPERTY,
        "SIGNATURE_VERSION_SYSTEM_PROPERTY should be correct");
    assertEquals(
        "aws.accessKeyId",
        MinioUtil.ACCESS_KEY_SYSTEM_PROPERTY,
        "ACCESS_KEY_SYSTEM_PROPERTY should be correct");
    assertEquals(
        "aws.secretKey",
        MinioUtil.SECRET_KEY_SYSTEM_PROPERTY,
        "SECRET_KEY_SYSTEM_PROPERTY should be correct");
    assertEquals("AWS_REGION", MinioUtil.AWS_REGION, "AWS_REGION should be correct");
    assertEquals("AWS_CONFIG_FILE", MinioUtil.AWS_CONFIG_FILE, "AWS_CONFIG_FILE should be correct");
    assertEquals(".aws", MinioUtil.AWS_FOLDER, "AWS_FOLDER should be correct");
    assertEquals("config", MinioUtil.CONFIG_FILE, "CONFIG_FILE should be correct");
  }
}
