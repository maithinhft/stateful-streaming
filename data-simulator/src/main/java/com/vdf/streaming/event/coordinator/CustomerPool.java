package com.vdf.streaming.event.coordinator;

import com.vdf.streaming.event.model.Customer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Quản lý tập khách hàng (Customer Pool 150 người theo context.md).
 * Đảm bảo các giao dịch tái sử dụng cùng danh sách khách hàng để giữ tính nhất quán.
 */
public class CustomerPool {
    private final List<Customer> customers = new ArrayList<>();
    private final Random rand;

    private static final String[] LAST_NAMES = {"Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Võ", "Đặng", "Bùi", "Đỗ"};
    private static final String[] MIDDLE_NAMES = {"Văn", "Thị", "Đức", "Hải", "Minh", "Thu", "Ngọc", "Hoàng", "Gia", "Thanh"};
    private static final String[] FIRST_NAMES = {"Anh", "Bình", "Cường", "Dũng", "Dương", "Huy", "Hương", "Hùng", "Linh", "Long", "Nam", "Nhung", "Phương", "Quân", "Sơn", "Thắng", "Thịnh", "Trang", "Tuấn", "Vy"};
    private static final String[] PROVINCES = {"Hà Nội", "TP Hồ Chí Minh", "Đà Nẵng", "Hải Phòng", "Cần Thơ", "Quảng Ninh", "Bình Dương", "Đồng Nai"};

    public CustomerPool(int poolSize, Random rand) {
        this.rand = rand;
        initPool(poolSize);
    }

    private void initPool(int poolSize) {
        for (int i = 1; i <= poolSize; i++) {
            int custId = 100000 + i;
            String msisdn = "84" + (980000000 + rand.nextInt(19999999));
            String name = LAST_NAMES[rand.nextInt(LAST_NAMES.length)] + " " +
                    MIDDLE_NAMES[rand.nextInt(MIDDLE_NAMES.length)] + " " +
                    FIRST_NAMES[rand.nextInt(FIRST_NAMES.length)];
            String idNo = "0" + (10000000000L + rand.nextInt(899999999));
            String gender = rand.nextBoolean() ? "M" : "F";
            int birthYear = 1975 + rand.nextInt(30);
            int birthMonth = 1 + rand.nextInt(12);
            int birthDay = 1 + rand.nextInt(28);
            String birthday = String.format("%04d-%02d-%02d", birthYear, birthMonth, birthDay);
            String address = PROVINCES[rand.nextInt(PROVINCES.length)];
            String accountNo = "VTM" + (20000000 + i);

            boolean isIos = rand.nextBoolean();
            String manufacturer = isIos ? "Apple" : (rand.nextBoolean() ? "Samsung" : "Xiaomi");
            String model = isIos ? (rand.nextBoolean() ? "iPhone 14 Pro" : "iPhone 15") : (rand.nextBoolean() ? "Galaxy S23" : "Redmi Note 12");
            String os = isIos ? "iOS" : "Android";
            String osVersion = isIos ? "17.4" : "14.0";
            String imei = "35" + (1000000000000L + rand.nextInt(899999999));
            String ipAddr = "118.70." + (10 + rand.nextInt(200)) + "." + (10 + rand.nextInt(200));
            String userSessionId = "sess_" + UUID.randomUUID().toString().substring(0, 18);
            long initialBalance = 1_000_000L + rand.nextInt(20_000_000);

            Customer cust = new Customer(custId, msisdn, name, idNo, "CCCD", "Căn cước công dân",
                    gender, birthday, address, accountNo, manufacturer, model, os, osVersion,
                    imei, ipAddr, userSessionId, initialBalance);
            customers.add(cust);
        }
    }

    public Customer getRandomCustomer() {
        return customers.get(ThreadLocalRandom.current().nextInt(customers.size()));
    }

    public List<Customer> getAll() {
        return customers;
    }
}

