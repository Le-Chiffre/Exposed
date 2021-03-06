package org.jetbrains.exposed.sql

import org.joda.time.DateTime
import java.sql.Date
import java.sql.PreparedStatement
import java.util.*
import org.jetbrains.exposed.dao.EntityID
import org.jetbrains.exposed.dao.IdTable


abstract class ColumnType(var isNullable: Boolean = false, var isAutoIncrement: Boolean = false) {
    public abstract fun sqlType(): String

    public open fun valueFromDB(value: Any): Any  = value

    public fun valueToString(value: Any?) : String {
        return when (value) {
            null -> {
                if (!isNullable) error("NULL in non-nullable column")
                "NULL"
            }

            is List<*> -> {
                value.map {valueToString(it)}.joinToString(",")
            }

            else ->  {
                nonNullValueToString (value)
            }
        }
    }

    public fun valueToDB(value: Any?): Any? = if (value != null) notNullValueToDB(value) else null

    public open fun notNullValueToDB(value: Any): Any  = value

    protected open fun nonNullValueToString(value: Any) : String {
        return notNullValueToDB(value).toString()
    }

    public open fun setParameter(stmt: PreparedStatement, index: Int, value: Any) {
        stmt.setObject(index, value)
    }

    override fun toString(): String {
        return sqlType()
    }
}

class EntityIDColumnType(val table: IdTable, autoinc: Boolean = false): ColumnType(autoinc) {
    override fun sqlType(): String  = "INT"

    override fun notNullValueToDB(value: Any): Any {
        return when (value) {
            is EntityID -> value.value
            is Int -> value
            else -> error("Unknown value for entity id: $value")
        }
    }

    override fun valueFromDB(value: Any): Any {
        return when (value) {
            is EntityID -> EntityID(value.value, table)
            else -> EntityID(value as Long, table)
        }
    }
}

class CharacterColumnType() : ColumnType() {
    override fun sqlType(): String  = "CHAR"

    override fun valueFromDB(value: Any): Any {
        return when(value) {
            is Char -> value
            is Number -> value.toChar()
            else -> error("Unexpected value of type Char: $value")
        }
    }
}

class ShortColumnType(autoinc: Boolean = false): ColumnType(autoinc) {
    override fun sqlType(): String  = "SMALLINT"

    override fun valueFromDB(value: Any): Any {
        return when(value) {
            is Short -> value
            is Int -> value.toShort()
            is Number -> value.toShort()
            else -> error("Unexpected value of type Int: $value")
        }
    }
}

class IntegerColumnType(autoinc: Boolean = false): ColumnType(autoinc) {

    override fun sqlType(): String  = "INT"

    override fun valueFromDB(value: Any): Any {
        return when(value) {
            is Int -> value
            is Number -> value.toInt()
            is Boolean -> if(value) 1 else 0
            else -> error("Unexpected value of type Int: $value")
        }
    }
}

class LongColumnType(autoinc: Boolean = false): ColumnType(autoinc) {
    override fun sqlType(): String  = "BIGINT"

    override fun valueFromDB(value: Any): Any {
        return when(value) {
            is Long -> value
            is Number -> value.toLong()
            else -> error("Unexpected value of type Long: $value")
        }
    }
}

class DecimalColumnType(val scale: Int, val precision: Int): ColumnType() {
    override fun sqlType(): String  = "DECIMAL($scale, $precision)"
}

class FloatColumnType(): ColumnType() {
    override fun sqlType(): String  = "FLOAT"

    override fun valueFromDB(value: Any): Any {
        return when(value) {
            is Float -> value
            is Int -> value.toFloat()
            is Number -> value.toFloat()
            else -> error("Unexpected value of type Int: $value")
        }
    }
}

class EnumerationColumnType<T:Enum<T>>(val klass: Class<T>): ColumnType() {
    override fun sqlType(): String  = "INT"

    override fun notNullValueToDB(value: Any): Any {
        return when (value) {
            is Int -> value
            is Enum<*> -> value.ordinal
            else -> error("$value is not valid for enum ${klass.name}")
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun valueFromDB(value: Any): Any {
        if (value is Enum<*>)
            return value as Enum<T>
        return klass.enumConstants!![value as Int]
    }
}

class DateColumnType(val time: Boolean): ColumnType() {
    override fun sqlType(): String  = if (time) "DATETIME" else "DATE"

    protected override fun nonNullValueToString(value: Any): String {
        if (value is String) return value

        val dateTime = when (value) {
            is DateTime -> value
            is Date -> DateTime(value.time)
            is java.sql.Timestamp -> DateTime(value.time)
            else -> error("Unexpected value: $value")
        } as DateTime

        if (time) {
            val zonedTime = dateTime.toDateTime(Database.timeZone)
            return "'${zonedTime.toString("YYYY-MM-dd HH:mm:ss.SSS", Locale.ROOT)}'"
        } else {
            val date = Date (dateTime.millis)
            return "'${date.toString()}'"
        }
    }

    override fun valueFromDB(value: Any): Any {
        if (value is Date) {
            return DateTime(value)
        }

        if (value is java.sql.Timestamp) {
            return DateTime(value.time)
        }

        return value
    }

    override fun notNullValueToDB(value: Any): Any {
        if (value is DateTime) {
            val millis = value.millis
            if (time) {
                return java.sql.Timestamp(millis)
            }
            else {
                return Date(millis)
            }
        }
        return value
    }
}

class TimestampColumnType: ColumnType() {
    override fun sqlType(): String = "TIMESTAMP"

    protected override fun nonNullValueToString(value: Any): String {
        if (value is String) return value

        val dateTime = when (value) {
            is DateTime -> value
            is Date -> DateTime(value.time)
            is java.sql.Timestamp -> DateTime(value.time)
            else -> error("Unexpected value: $value")
        } as DateTime

        val zonedTime = dateTime.toDateTime(Database.timeZone)
        return "'${zonedTime.toString("YYYY-MM-dd HH:mm:ss.SSS", Locale.ROOT)}'"
    }

    override fun valueFromDB(value: Any): Any {
        if (value is Date) {
            return DateTime(value)
        }

        if (value is java.sql.Timestamp) {
            return DateTime(value.time)
        }

        return value
    }

    override fun notNullValueToDB(value: Any): Any {
        if(value is DateTime) {
            val millis = value.millis
            return java.sql.Timestamp(millis)
        }
        return value
    }
}

class StringColumnType(val length: Int = 65535, val collate: String? = null): ColumnType() {
    override fun sqlType(): String  {
        val ddl = StringBuilder()

        ddl.append(when (length) {
            in 1..255 -> "VARCHAR($length)"
            else -> "TEXT"
        })

        if (collate != null) {
            ddl.append(" COLLATE $collate")
        }

        return ddl.toString()
    }

    val charactersToEscape = hashMapOf(
            '\'' to "\'\'",
            //            '\"' to "\"\"", // no need to escape double quote as we put string in single quotes
            '\r' to "\\r",
            '\n' to "\\n")

    protected override fun nonNullValueToString(value: Any): String {
        val beforeEscaping = value.toString()
        val sb = StringBuilder(beforeEscaping.length +2)
        sb.append('\'')
        for (c in beforeEscaping) {
            if (charactersToEscape.containsKey(c))
                sb.append(charactersToEscape[c])
            else
                sb.append(c)
        }
        sb.append('\'')
        return sb.toString()
    }

    override fun valueFromDB(value: Any): Any {
        if (value is java.sql.Clob) {
            return value.characterStream.readText()
        }
        return value
    }
}

class BlobColumnType(): ColumnType() {
    override fun sqlType(): String  = "BLOB"

    override fun nonNullValueToString(value: Any): String {
        return "?"
    }
}

class BooleanColumnType() : ColumnType() {
    override fun sqlType(): String  = "BIT"

    override fun nonNullValueToString(value: Any): String {
        return if (value as Boolean) "1" else "0"
    }

    override fun valueFromDB(value: Any): Any {
        return when(value) {
            is Boolean -> value
            is Float -> value != 0f
            is Int -> value != 0
            is Long -> value != 0L
            is Number -> value != 0
            else -> error("Unexpected value of type Boolean: $value")
        }
    }
}