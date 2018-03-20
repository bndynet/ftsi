package net.bndy.ftsi;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Indexable {

    boolean isKey() default false;
    boolean ignore() default false;
    IndexType stringIndexType() default IndexType.FUZZY;
}
