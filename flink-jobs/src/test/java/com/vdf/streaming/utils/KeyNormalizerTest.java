package com.vdf.streaming.utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class KeyNormalizerTest {

    @Test
    @DisplayName("Chuẩn hóa số điện thoại nội địa bắt đầu bằng 0")
    void testNormalizeLeadingZero() {
        assertEquals("+84981234567", KeyNormalizer.normalize("0981234567"));
        assertEquals("+84901234567", KeyNormalizer.normalize("0901234567"));
        assertEquals("+84381234567", KeyNormalizer.normalize("0381234567"));
    }

    @Test
    @DisplayName("Chuẩn hóa số bắt đầu bằng 84 không có dấu +")
    void testNormalizePrefix84WithoutPlus() {
        assertEquals("+84981234567", KeyNormalizer.normalize("84981234567"));
    }

    @Test
    @DisplayName("Số đã có dấu + chuẩn quốc tế E.164")
    void testAlreadyNormalized() {
        assertEquals("+84981234567", KeyNormalizer.normalize("+84981234567"));
        assertEquals("+14155552671", KeyNormalizer.normalize("+14155552671"));
    }

    @Test
    @DisplayName("Số chứa ký tự format lạ (dấu cách, gạch ngang, chấm, ngoặc)")
    void testDirtyCharactersCleaning() {
        assertEquals("+84981234567", KeyNormalizer.normalize(" 098-123-4567 "));
        assertEquals("+84981234567", KeyNormalizer.normalize("(098) 123.4567"));
        assertEquals("+84981234567", KeyNormalizer.normalize("+84 98 123 4567"));
    }

    @Test
    @DisplayName("Số quốc tế dạng 00...")
    void testInternationalPrefixDoubleZero() {
        assertEquals("+84981234567", KeyNormalizer.normalize("0084981234567"));
    }

    @Test
    @DisplayName("Số nội địa 9 số không có số 0 đầu")
    void testLocalNineDigits() {
        assertEquals("+84981234567", KeyNormalizer.normalize("981234567"));
    }

    @Test
    @DisplayName("Số không hợp lệ hoặc rác")
    void testInvalidNumbers() {
        assertNull(KeyNormalizer.normalize(null));
        assertNull(KeyNormalizer.normalize(""));
        assertNull(KeyNormalizer.normalize("   "));
        assertNull(KeyNormalizer.normalize("12345"));
        assertNull(KeyNormalizer.normalize("not_a_phone_number"));
    }

    @Test
    @DisplayName("Kiểm tra hàm isValidE164")
    void testIsValidE164() {
        assertTrue(KeyNormalizer.isValidE164("+84981234567"));
        assertTrue(KeyNormalizer.isValidE164("+14155552671"));
        assertFalse(KeyNormalizer.isValidE164("0981234567"));
        assertFalse(KeyNormalizer.isValidE164("+0981234567"));
        assertFalse(KeyNormalizer.isValidE164(null));
    }

    public static void main(String[] args) {
        System.out.println("=== Running KeyNormalizer Tests ===");
        KeyNormalizerTest test = new KeyNormalizerTest();
        test.testNormalizeLeadingZero();
        test.testNormalizePrefix84WithoutPlus();
        test.testAlreadyNormalized();
        test.testDirtyCharactersCleaning();
        test.testInternationalPrefixDoubleZero();
        test.testLocalNineDigits();
        test.testInvalidNumbers();
        test.testIsValidE164();
        System.out.println(">>> ALL 8 KeyNormalizer tests PASSED! <<<");
    }
}
