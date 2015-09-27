package kotlin.sql.generate;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that a value type should be generated for this table.
 * The table can then automatically generate instances of that type using lookup() and find().
 * Can only be applied to classes extending BaseLookupTable.
 * If set, the name indicates the name of the generated class.
 * Otherwise, the name becomes the source class with with Value appended, i.e. ImageValue.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.CLASS)
public @interface Value {
    String name() default("");
}
