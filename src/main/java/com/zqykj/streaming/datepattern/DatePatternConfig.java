package com.zqykj.streaming.datepattern;

import java.io.Serializable;

public class DatePatternConfig implements Serializable {

    public static final String DATE_PATTERN_CODE = "system.elpmodel.entity.property.transform.supported.datetime.format";
    public static final String CODE = "code";
    public static final String SPACK_MARK = "@";

    private String code;
    private String name;
    private DatePattern value;

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DatePattern getValue() {
        return value;
    }

    public void setValue(DatePattern value) {
        this.value = value;
    }
}