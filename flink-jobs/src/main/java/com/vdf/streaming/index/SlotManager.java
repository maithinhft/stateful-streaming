package com.vdf.streaming.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.roaringbitmap.RoaringBitmap;
import com.vdf.streaming.models.CompiledRuleEnvelope;

/**
 * SlotManager - Quản lý ánh xạ giữa Rule ID và vị trí bit (slot ID) trong RoaringBitmap.
 * Hỗ trợ cấp phát lại các slot đã bị xóa (bằng cách theo dõi các slot trống).
 */
public class SlotManager implements Serializable {

    private static final long serialVersionUID = 1L;

    // Danh sách lưu trữ rule theo index (slotId)
    private final ArrayList<CompiledRuleEnvelope> slotToRule;
    
    // Map tra cứu ngược từ Rule ID sang slotId
    private final Map<String, Integer> ruleToSlot;
    
    // Bitmap theo dõi các slot trống (Bit=1: trống, Bit=0: đang sử dụng)
    private final RoaringBitmap freeSlotsBitmap;
    
    // Slot ID cao nhất đã từng được cấp phát
    private int maxAllocatedIndex;

    /**
     * Khởi tạo SlotManager.
     */
    public SlotManager() {
        this.slotToRule = new ArrayList<>();
        this.ruleToSlot = new HashMap<>();
        this.freeSlotsBitmap = new RoaringBitmap();
        this.maxAllocatedIndex = -1;
    }

    /**
     * Constructor copy để hỗ trợ Copy-On-Write pattern.
     */
    private SlotManager(SlotManager other) {
        this.slotToRule = new ArrayList<>(other.slotToRule);
        this.ruleToSlot = new HashMap<>(other.ruleToSlot);
        this.freeSlotsBitmap = other.freeSlotsBitmap.clone();
        this.maxAllocatedIndex = other.maxAllocatedIndex;
    }

    /**
     * Cấp phát slot ID cho một Rule.
     * @param ruleId ID của Rule
     * @return slot ID được cấp phát
     */
    public int allocateSlot(String ruleId) {
        // Nếu đã tồn tại, trả về slot cũ để update
        if (ruleToSlot.containsKey(ruleId)) {
            return ruleToSlot.get(ruleId);
        }

        int slotId;
        // Tìm slot trống trong freeSlotsBitmap
        if (!freeSlotsBitmap.isEmpty()) {
            slotId = freeSlotsBitmap.first();
            // Xóa bit (đánh dấu đã sử dụng, bit=0)
            freeSlotsBitmap.remove(slotId);
        } else {
            // Không có slot trống, cấp phát mới
            maxAllocatedIndex++;
            slotId = maxAllocatedIndex;
        }

        // Cập nhật mapping
        ruleToSlot.put(ruleId, slotId);
        return slotId;
    }

    /**
     * Giải phóng slot của một Rule (khi xóa Rule).
     * @param ruleId ID của Rule cần xóa
     */
    public void releaseSlot(String ruleId) {
        Integer slotId = ruleToSlot.remove(ruleId);
        if (slotId != null) {
            // Đặt bit 1 vào freeSlotsBitmap (đánh dấu trống)
            freeSlotsBitmap.add(slotId);
            if (slotId < slotToRule.size()) {
                slotToRule.set(slotId, null);
            }
        }
    }

    /**
     * Gán Rule vào một slot.
     * @param slotId ID của slot
     * @param rule Đối tượng Rule
     */
    public void setRule(int slotId, CompiledRuleEnvelope rule) {
        // Đảm bảo ArrayList đủ kích thước
        while (slotToRule.size() <= slotId) {
            slotToRule.add(null);
        }
        slotToRule.set(slotId, rule);
    }

    /**
     * Lấy Rule theo slot ID.
     * @param slotId ID của slot
     * @return Đối tượng Rule hoặc null nếu không có
     */
    public CompiledRuleEnvelope getRule(int slotId) {
        if (slotId >= 0 && slotId < slotToRule.size()) {
            return slotToRule.get(slotId);
        }
        return null;
    }

    /**
     * Lấy slot ID từ Rule ID.
     * @param ruleId ID của Rule
     * @return slot ID hoặc -1 nếu không tồn tại
     */
    public int getSlotId(String ruleId) {
        return ruleToSlot.getOrDefault(ruleId, -1);
    }

    /**
     * Kiểm tra xem Rule có tồn tại không.
     * @param ruleId ID của Rule
     * @return true nếu tồn tại, ngược lại false
     */
    public boolean hasRule(String ruleId) {
        return ruleToSlot.containsKey(ruleId);
    }

    /**
     * Lấy bitmap các slot trống (sử dụng cho AND NOT mask trong quá trình match event).
     * @return RoaringBitmap các slot trống
     */
    public RoaringBitmap getFreeSlotsBitmap() {
        return freeSlotsBitmap;
    }

    /**
     * Lấy giá trị slot ID cao nhất đã từng cấp phát.
     * @return max allocated index
     */
    public int getMaxAllocatedIndex() {
        return maxAllocatedIndex;
    }

    /**
     * Tạo bản sao sâu (deep copy) để hỗ trợ Copy-On-Write pattern.
     * @return Bản sao của SlotManager
     */
    public SlotManager deepCopy() {
        return new SlotManager(this);
    }
}
