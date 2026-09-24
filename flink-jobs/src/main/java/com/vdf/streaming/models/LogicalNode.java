package com.vdf.streaming.models;

import java.util.List;

/**
 * Nút logic (AND/OR).
 */
public record LogicalNode(
        String type,
        List<ConditionNode> children
) implements ConditionNode {
}
