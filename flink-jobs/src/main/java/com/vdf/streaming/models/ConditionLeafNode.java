package com.vdf.streaming.models;

/**
 * Nút lá điều kiện.
 */
public record ConditionLeafNode(
        Expression expression
) implements ConditionNode {
}
