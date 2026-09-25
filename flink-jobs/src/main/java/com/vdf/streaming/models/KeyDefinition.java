package com.vdf.streaming.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class KeyDefinition implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("field")
    private String field = "msisdn";

    @JsonProperty("key_type")
    private String keyType = "PHONE_E164";

    @JsonProperty("allow_null")
    private boolean allowNull = false;

    @JsonProperty("auto_normalize")
    private boolean autoNormalize = true;

    @JsonProperty("default_country_code")
    private String defaultCountryCode = "84";

    @JsonProperty("regex_pattern")
    private String regexPattern;

    @JsonProperty("allowed_country_codes")
    private List<String> allowedCountryCodes;

    public KeyDefinition() {}

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getKeyType() {
        return keyType;
    }

    public void setKeyType(String keyType) {
        this.keyType = keyType;
    }

    public boolean isAllowNull() {
        return allowNull;
    }

    public void setAllowNull(boolean allowNull) {
        this.allowNull = allowNull;
    }

    public boolean isAutoNormalize() {
        return autoNormalize;
    }

    public void setAutoNormalize(boolean autoNormalize) {
        this.autoNormalize = autoNormalize;
    }

    public String getDefaultCountryCode() {
        return defaultCountryCode;
    }

    public void setDefaultCountryCode(String defaultCountryCode) {
        this.defaultCountryCode = defaultCountryCode;
    }

    public String getRegexPattern() {
        return regexPattern;
    }

    public void setRegexPattern(String regexPattern) {
        this.regexPattern = regexPattern;
    }

    public List<String> getAllowedCountryCodes() {
        return allowedCountryCodes;
    }

    public void setAllowedCountryCodes(List<String> allowedCountryCodes) {
        this.allowedCountryCodes = allowedCountryCodes;
    }
}
