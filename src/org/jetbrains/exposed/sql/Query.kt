package org.jetbrains.exposed.sql

import com.rimmer.metrics.EventType
import java.sql.ResultSet
import java.util.*
import org.jetbrains.exposed.dao.EntityCache
import org.jetbrains.exposed.dao.IdTable

fun batchSelect(vararg query: Query) {
    val queryString = StringBuilder()
    val builder = QueryBuilder(true)
    query.forEach {
        queryString.append(it.toSQL(builder, it.isCount))
        queryString.append(';')
    }

    builder.executeBatchQuery(query[0].session, queryString.toString()) {
        i, result ->
        val q = query[i]
        if(q.isCount) {
            result.next()
            q.countCache = result.getInt(1)
        } else {
            q.resultCache = q.makeIterator(result)
        }
    }
}

public class ResultRow(size: Int, private val fieldIndex: Map<Expression<*>, Int>) {
    val data = arrayOfNulls<Any?>(size)

    /**
     * Function might returns null. Use @tryGet if you don't sure of nullability (e.g. in left-join cases)
     */
    @Suppress("UNCHECKED_CAST")
    operator fun <T> get(c: Expression<T>) : T {
        val d:Any? = when {
            fieldIndex.containsKey(c) -> data[fieldIndex[c]!!]
            else -> error("${c.toSQL(QueryBuilder(false))} is not in record set")
        }

        return d?.let {
            (c as? ExpressionWithColumnType<*>)?.columnType?.valueFromDB(it) ?: it
        } as T
    }

    fun getAll(columns: Collection<Expression<*>>) = columns.map {get(it)}.toTypedArray()

    operator fun <T> set(c: Expression<T>, value: T) {
        val index = fieldIndex[c] ?: error("${c.toSQL(QueryBuilder(false))} is not in record set")
        data[index] = value
    }

    fun<T> hasValue (c: Expression<T>) : Boolean {
        return fieldIndex[c]?.let{data[it]} != null;
    }

    fun contains(c: Expression<*>) = fieldIndex.containsKey(c)

    fun <T> tryGet(c: Expression<T>): T? {
        return if (hasValue(c)) get(c) else null
    }

    override fun toString(): String {
        return fieldIndex.map { "${it.key.toSQL(QueryBuilder(false))}=${data[it.value]}" }.joinToString()
    }

    companion object {
        fun create(rs: ResultSet, fields: List<Expression<*>>, fieldsIndex: Map<Expression<*>, Int>) : ResultRow {
            val size = fieldsIndex.size
            val answer = ResultRow(size, fieldsIndex)

            fields.forEachIndexed{ i, f ->
                answer.data[i] = when {
                    f is Column<*> && f.columnType is BlobColumnType -> rs.getBlob(i + 1)
                    else -> rs.getObject(i + 1)
                }
            }
            return answer
        }
    }
}

open class Query(val session: Session, val set: FieldSet, val where: Op<Boolean>?, val isCount: Boolean = false): SizedIterable<ResultRow> {
    val groupedByColumns = ArrayList<Expression<*>>();
    val orderByColumns = ArrayList<Pair<Expression<*>, Boolean>>();
    var having: Op<Boolean>? = null;
    var limit: Int? = null
    var offset: Int? = null
    var forUpdate: Boolean = session.selectsForUpdate && session.vendorSupportsForUpdate()

    var emptyCache: Boolean? = null
    var countCache: Int? = null
    var resultCache: Iterator<ResultRow>? = null


    fun toSQL(queryBuilder: QueryBuilder, count: Boolean = false) : String {
        val source = set.source.describe(session)
        val event = session.db.metrics?.startEvent(EventType.mysql_generate, "SELECT $source")

        val sql = StringBuilder("SELECT ")

        with(sql) {
            if (count) {
                append("COUNT(*)")
            } else {
                val fields = LinkedHashSet(set.fields)
                append(fields.map {it.toSQL(queryBuilder)}.joinToString(", ", "", ""))
            }

            append(" FROM ")
            append(source)

            if (where != null) {
                append(" WHERE ")
                append(where.toSQL(queryBuilder))
            }

            if (!count) {
                if (groupedByColumns.isNotEmpty()) {
                    append(" GROUP BY ")
                    append((groupedByColumns.map {it.toSQL(queryBuilder)}).joinToString(", ", "", ""))
                }

                if (having != null) {
                    append(" HAVING ")
                    append(having!!.toSQL(queryBuilder))
                }

                if (orderByColumns.isNotEmpty()) {
                    append(" ORDER BY ")
                    append((orderByColumns.map { "${it.first.toSQL(queryBuilder)} ${if(it.second) "ASC" else "DESC"}" }).joinToString(", ", "", ""))
                }

                if (limit != null) {
                    append(" LIMIT ")
                    append(limit!!)

                    if(offset != null) {
                        append(" OFFSET ")
                        append(offset!!)
                    }
                }
            }

            if (forUpdate) {
                append(" FOR UPDATE")
            }
        }

        val string = sql.toString()
        session.db.metrics?.endEvent(event!!)
        return string
    }

    override fun forUpdate() : Query {
        this.forUpdate = true
        return this
    }

    override fun notForUpdate(): Query {
        forUpdate = false
        return this
    }

    infix fun groupBy(column: Expression<*>): Query {
        groupedByColumns.add(column)
        return this
    }

    fun groupBy(vararg columns: Expression<*>): Query {
        for (column in columns) {
            groupedByColumns.add(column)
        }
        return this
    }

    infix fun having (op: SqlExpressionBuilder.() -> Op<Boolean>) : Query {
        val oop = Op.build { op() }
        if (having != null) {
            val fake = QueryBuilder(false)
            error ("HAVING clause is specified twice. Old value = '${having!!.toSQL(fake)}', new value = '${oop.toSQL(fake)}'")
        }
        having = oop;
        return this;
    }

    infix fun orderBy (column: Expression<*>) = orderBy(column, true)

    fun orderBy (column: Expression<*>, isAsc: Boolean = true) : Query {
        orderByColumns.add(column to isAsc)
        return this
    }

    fun orderBy (vararg columns: Pair<Column<*>,Boolean>) : Query {
        for (pair in columns) {
            orderByColumns.add(pair)
        }
        return this
    }

    infix override fun limit(n: Int): Query {
        this.limit = n
        return this
    }

    infix override fun offset(n: Int): Query {
        this.offset = n
        return this
    }

    fun makeIterator(set: ResultSet): Iterator<ResultRow> = ResultIterator(set)

    private inner class ResultIterator(val rs: ResultSet): Iterator<ResultRow> {
        private var hasNext: Boolean? = null
        private val fieldsIndex = HashMap<Expression<*>, Int>()

        init {
            set.fields.forEachIndexed { idx, field ->
                fieldsIndex[field] = idx
            }
        }

        operator public override fun next(): ResultRow {
            if (hasNext == null) hasNext()
            if (hasNext == false) throw NoSuchElementException()
            hasNext = null
            return ResultRow.Companion.create(rs, set.fields, fieldsIndex)
        }

        public override fun hasNext(): Boolean {
            if (hasNext == null) hasNext = rs.next()
            return hasNext!!
        }
    }

    private fun flushEntities() {
        // Flush data before executing query or results may be unpredictable
        val tables = set.source.columns.map { it.table }.filterIsInstance(IdTable::class.java).toSet()
        EntityCache.getOrCreate(session).flush(tables)
    }

    operator public override fun iterator(): Iterator<ResultRow> {
        if(resultCache == null) {
            flushEntities()
            val builder = QueryBuilder(true)
            val sql = toSQL(builder)
            resultCache = ResultIterator(builder.executeQuery(session, sql))
        }
        return resultCache!!
    }

    public override fun count(): Int {
        if(countCache == null) {
            flushEntities()

            val builder = QueryBuilder(true)
            val sql = toSQL(builder, true)

            val rs = builder.executeQuery(session, sql)
            rs.next()
            countCache = rs.getInt(1)
        }
        return countCache!!
    }

    public override fun empty(): Boolean {
        if(emptyCache == null) {
            flushEntities()
            val builder = QueryBuilder(true)

            val selectOneRowStatement = run {
                val oldLimit = limit
                try {
                    limit = 1
                    toSQL(builder, false)
                } finally {
                    limit = oldLimit
                }
            }
            // Execute query itself
            val rs = builder.executeQuery(session, selectOneRowStatement)
            emptyCache = !rs.next()
        }
        return emptyCache!!
    }
}