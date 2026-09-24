package com.vdf.streaming.index;

import org.roaringbitmap.RoaringBitmap;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Đại diện cho Dual-Index cho một cặp (source, schemaVersion) cụ thể.
 * Lưu trữ chỉ mục chính xác (exact index) và chỉ mục phức tạp (complex index) 
 * để tối ưu hóa việc tìm kiếm các luật (rules) phù hợp với một sự kiện.
 */
public class SourceVersionIndex implements Serializable {
    private static final long serialVersionUID = 1L;

    // Key = "field:value", Value = bitmap các slot của luật (rule slots)
    private final Map<String, RoaringBitmap> exactIndex;
    
    // Single bitmap cho các luật sử dụng các toán tử phức tạp 
    // (>, <, !=, BETWEEN, CONTAINS, ENDS_WITH, STARTS_WITH, v.v.)
    private final RoaringBitmap complexIndex;

    public SourceVersionIndex() {
        this.exactIndex = new HashMap<>();
        this.complexIndex = new RoaringBitmap();
    }

    private SourceVersionIndex(Map<String, RoaringBitmap> exactIndex, RoaringBitmap complexIndex) {
        this.exactIndex = new HashMap<>(exactIndex.size());
        for (Map.Entry<String, RoaringBitmap> entry : exactIndex.entrySet()) {
            this.exactIndex.put(entry.getKey(), entry.getValue().clone());
        }
        this.complexIndex = complexIndex.clone();
    }

    /**
     * Thêm một mục chính xác (exact entry) vào chỉ mục.
     * Tạo key = field + ":" + value và thêm slotId vào bitmap tương ứng.
     */
    public void addExactEntry(String field, String value, int slotId) {
        String key = field + ":" + value;
        exactIndex.computeIfAbsent(key, k -> new RoaringBitmap()).add(slotId);
    }

    /**
     * Thêm danh sách các mục chính xác (cho toán tử IN).
     * Làm phẳng (flatten) danh sách và gọi addExactEntry cho từng giá trị.
     */
    public void addExactEntries(String field, List<?> values, int slotId) {
        if (values != null) {
            for (Object value : values) {
                if (value != null) {
                    addExactEntry(field, value.toString(), slotId);
                }
            }
        }
    }

    /**
     * Thêm một mục dataset (cho toán tử IN_DATASET).
     * Tạo key = field + ":" + datasetId và thêm slotId vào bitmap.
     */
    public void addDatasetEntry(String field, String datasetId, int slotId) {
        String key = field + ":" + datasetId;
        exactIndex.computeIfAbsent(key, k -> new RoaringBitmap()).add(slotId);
    }

    /**
     * Đánh dấu slotId sử dụng các toán tử phức tạp.
     */
    public void addComplexEntry(int slotId) {
        complexIndex.add(slotId);
    }

    /**
     * Xóa slotId khỏi tất cả các bitmap trong exactIndex và complexIndex.
     * Đồng thời dọn dẹp các bitmap rỗng khỏi exactIndex.
     */
    public void removeSlot(int slotId) {
        complexIndex.remove(slotId);
        
        exactIndex.entrySet().removeIf(entry -> {
            RoaringBitmap bitmap = entry.getValue();
            bitmap.remove(slotId);
            return bitmap.isEmpty();
        });
    }

    /**
     * Tìm kiếm các slot ứng viên dựa trên các trường của sự kiện và danh sách các slot còn trống.
     * Kết hợp (OR) tất cả các bitmap phù hợp từ exactIndex cùng với complexIndex.
     * Sau đó loại bỏ (AND NOT) các slot đã bị xóa (dựa vào freeSlots).
     */
    public RoaringBitmap queryCandidates(Map<String, Object> eventFields, RoaringBitmap freeSlots) {
        RoaringBitmap result = new RoaringBitmap();
        
        // Gộp kết quả từ exactIndex
        if (eventFields != null) {
            for (Map.Entry<String, Object> entry : eventFields.entrySet()) {
                if (entry.getValue() != null) {
                    String key = entry.getKey() + ":" + entry.getValue().toString();
                    RoaringBitmap bitmap = exactIndex.get(key);
                    if (bitmap != null) {
                        result.or(bitmap);
                    }
                }
            }
        }
        
        // Gộp kết quả từ complexIndex
        result.or(complexIndex);
        
        // Loại bỏ các rule đã bị xóa (các slot rảnh rỗi)
        if (freeSlots != null) {
            result.andNot(freeSlots);
        }
        
        return result;
    }

    /**
     * Tạo một bản sao sâu (deep copy) của chỉ mục phục vụ cho kỹ thuật COW (Copy-On-Write).
     */
    public SourceVersionIndex deepCopy() {
        return new SourceVersionIndex(this.exactIndex, this.complexIndex);
    }

    public void printDebugInfo() {
        System.out.println("      * exactIndex (keys=" + exactIndex.size() + "):");
        for (Map.Entry<String, RoaringBitmap> entry : exactIndex.entrySet()) {
            System.out.println("        - " + entry.getKey() + " -> Slots: " + entry.getValue().toString());
        }
        System.out.println("      * complexIndex -> Slots: " + complexIndex.toString());
    }
}
