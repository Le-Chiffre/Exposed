package kotlin.sql

import com.rimmer.DBType
import java.util.*

class BatchInsertQuery(val table: Table, val isIgnore: Boolean = false, val isReplace: Boolean = false) {
    val data = ArrayList<LinkedHashMap<Column<*>, Any?>>()

    fun addBatch() {
        data.add(LinkedHashMap())
    }

    operator fun <T> set(column: Column<T>, value: T) {
        val values = data.last()

        if (values.containsKey(column)) {
            error("$column is already initialized")
        }

        values.put(column, column.columnType.valueToDB(value))
    }

    fun execute(session: Session): List<Long> {
        if (data.isEmpty())
            return emptyList()

        val generatedKeys = ArrayList<Long>()
        val (auto, columns) = table.columns.partition { it.columnType.isAutoIncrement }
        val ignore = if (isIgnore) "IGNORE" else ""
        val insert = if (isReplace && Session.get().vendor != DatabaseVendor.MySql) "REPLACE" else "INSERT"
        var sql = StringBuilder("$insert $ignore INTO ${session.identity(table)}")

        sql.append(" (")
        sql.append((columns.map{ session.identity(it) }).joinToString(", "))
        sql.append(") ")

        sql.append("VALUES ")
        val paramsPlaceholder = columns.map{ "?" }.joinToString(", ", prefix = "(", postfix = "),")
        sql.append(paramsPlaceholder.repeat(data.size).removeSuffix(","))

        if (isReplace && Session.get().vendorCompatibleWith() == DatabaseVendor.MySql) {
            sql.append("ON DUPLICATE KEY UPDATE ")
            sql.append(columns.map {"${session.identity(it)} = VALUES(${session.identity(it)})"}.joinToString(", "))
        }

        try {
            val sqlText = sql.toString()

            session.execBatch {
                val stmt = session.prepareStatement(sqlText, auto.map{session.identity(it)})

                val args = arrayListOf<Pair<ColumnType, Any?>>()
                for ((i, d) in data.withIndex()) {
                    columns.mapTo(args) {it.columnType to (d[it] ?: it.defaultValue)}
                    stmt.fillParameters(columns, d, i)
                }

                log(sqlText, args)

                val metricsId = session.db.metrics?.startQuery(DBType.mysql, sqlText)
                val count = stmt.executeUpdate()
                session.db.metrics?.endQuery(metricsId!!)

                assert(count == data.size) { "Number of results don't match number of entries in batch" }

                if (auto.isNotEmpty()) {
                    val rs = stmt.generatedKeys!!
                    while (rs.next()) {
                        generatedKeys.add(rs.getLong(1))
                    }

                    if (generatedKeys.size == 1 && count > 1) {
                        // H2 only returns one last generated keys...
                        var id = generatedKeys.first()

                        while (generatedKeys.size < count) {
                            id -= 1
                            generatedKeys.add(0, id)
                        }
                    }

                    assert(generatedKeys.isEmpty() || generatedKeys.size == count) { "Number of autoincs doesn't match number of batch entries" }
                }
            }
        }
        catch (e: Exception) {
            println("BAD SQL: $sql")
            throw e
        }

        return generatedKeys
    }
}
