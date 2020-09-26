package com.lwwale.exception;

import com.lwwale.models.FunctionProperty;

public class MandatoryPropertyMissingException extends ApplicationException {
    private final String propertyName;
    private final String propertyDescription;

    public MandatoryPropertyMissingException(FunctionProperty functionProperty) {
        this(functionProperty.getPropertyName(), functionProperty.getPropertyDescription());
    }

    public MandatoryPropertyMissingException(String propertyName, String propertyDescription) {
        super("MandatoryProperty [" + propertyName + "]  Missing. Property Description :  " + propertyDescription);
        this.propertyName = propertyName;
        this.propertyDescription = propertyDescription;
    }

    public MandatoryPropertyMissingException(String message, String propertyName, String propertyDescription) {
        super(message);
        this.propertyName = propertyName;
        this.propertyDescription = propertyDescription;
    }

    public MandatoryPropertyMissingException(String message, Throwable cause, String propertyName, String propertyDescription) {
        super(message, cause);
        this.propertyName = propertyName;
        this.propertyDescription = propertyDescription;
    }

    public MandatoryPropertyMissingException(Throwable cause, String propertyName, String propertyDescription) {
        super("MandatoryProperty [" + propertyName + "]  Missing. Property Description :  " + propertyDescription, cause);
        this.propertyName = propertyName;
        this.propertyDescription = propertyDescription;
    }

    public MandatoryPropertyMissingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace, String propertyName, String propertyDescription) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.propertyName = propertyName;
        this.propertyDescription = propertyDescription;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public String getPropertyDescription() {
        return propertyDescription;
    }
}
