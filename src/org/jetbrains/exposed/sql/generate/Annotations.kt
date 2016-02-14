package org.jetbrains.exposed.sql.generate

/**
 * Indicates that a value type should be generated for this table.
 * The table can then automatically generate instances of that type using lookup() and find().
 * Can only be applied to classes extending BaseLookupTable.
 * If set, the name indicates the name of the generated class.
 * Otherwise, the name becomes the source class with with Value appended, i.e. ImageValue.
 */
@Target(AnnotationTarget.CLASS, AnnotationTarget.FILE)
@Retention(AnnotationRetention.BINARY)
annotation class Value(val value: String = (""))
