package com.tzw.core.util;

/**
 * 占位类，保证 core 包结构存在且可编译。
 * 后续迁入真实工具类替换。
 */
public final class Placeholder {
    private Placeholder() {}

    public static String module() {
        return "core";
    }
}
