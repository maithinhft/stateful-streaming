package com.vdf.streaming.models;

import java.io.Serializable;

/**
 * Khóa kết nối.
 */
public record JoinKey(
        String leftField,
        String rightField
) implements Serializable {
}
