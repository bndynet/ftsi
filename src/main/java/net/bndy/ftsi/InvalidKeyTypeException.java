package net.bndy.ftsi;

public class InvalidKeyTypeException extends Exception {
    private String className;

    public String getClassName() {
        return className;
    }

    public <T> InvalidKeyTypeException(Class<T> clazz) {
        this.className = clazz.getName();
    }

    @Override
    public String getMessage() {
        return "The data type of " + this.className + " key field MUST be String.";
    }
}
