package com.vdf.streaming.models;

import java.io.Serializable;

/**
 * Giao diện đại diện cho các nút trong cây điều kiện.
 */
public sealed interface ConditionNode extends Serializable permits LogicalNode, ConditionLeafNode, SequenceNode {
}
