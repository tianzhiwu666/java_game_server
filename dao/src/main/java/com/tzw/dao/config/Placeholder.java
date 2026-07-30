package com.tzw.dao.config;

/**
 * 占位类，保证 dao 包结构存在且可编译。
 * 后续迁入真实数据访问实现替换。
 */
public final class Placeholder {
    private Placeholder() {}

    public static String module() {
        return "dao";
    }
}
