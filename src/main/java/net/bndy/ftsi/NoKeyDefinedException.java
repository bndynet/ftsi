package net.bndy.ftsi;

public class NoKeyDefinedException extends Exception {

    private String className;

    public String getClassName() {
        return className;
    }

    public <T> NoKeyDefinedException(Class<T> clazz) {
        this.className = clazz.getName();
    }

    @Override
    public String getMessage() {
        return "No key definition for " + this.className + ". You can use @Indexable(isKey=true/false) to specify key field.";
    }
}
