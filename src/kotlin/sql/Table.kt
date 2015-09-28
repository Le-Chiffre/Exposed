package kotlin.sql

import org.joda.time.DateTime
import java.lang
import java.lang.reflect.Constructor
import java.math.BigDecimal
import java.sql.Blob
import java.util.*
import kotlin.dao.EntityID
import kotlin.dao.IdTable
import kotlin.reflect.KCallable
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.jvm.javaConstructor
import kotlin.reflect.jvm.javaType

interface FieldSet {
    val fields: List<Expression<*>>
    val source: ColumnSet
}

abstract class ColumnSet(): FieldSet {
    abstract val columns: List<Column<*>>
    override val fields: List<Expression<*>> get() = columns
    override val source = this

    abstract fun describe(s: Session): String

    fun slice(vararg columns: Expression<*>): FieldSet = Slice(this, listOf(*columns))
    fun slice(columns: List<Expression<*>>): FieldSet = Slice(this, columns)
}

class Slice(override val source: ColumnSet, override val fields: List<Expression<*>>): FieldSet

enum class JoinType {
    INNER,
    LEFT,
    RIGHT,
    FULL
}

fun Table.join (otherTable: Table) : Join {
    return Join (this, otherTable, JoinType.INNER)
}

fun Table.join (otherTable: Table, joinType: JoinType, onColumn: Column<*>? = null, otherColumn: Column<*>? = null, additionalConstraint: (SqlExpressionBuilder.()->Op<Boolean>)? = null) : Join {
    return Join (this, otherTable, joinType, onColumn, otherColumn, additionalConstraint)
}

fun Table.innerJoin (otherTable: Table) : Join {
    return Join (this, otherTable, JoinType.INNER)
}

fun Table.leftJoin (otherTable: Table) : Join {
    return Join (this, otherTable, JoinType.LEFT)
}

class Join (val table: Table) : ColumnSet() {

    public constructor(table: Table, otherTable: Table, joinType: JoinType = JoinType.INNER, onColumn: Column<*>? = null, otherColumn: Column<*>? = null, additionalConstraint: (SqlExpressionBuilder.()->Op<Boolean>)? = null) : this(table) {
        val new = if (onColumn != null && otherColumn != null) {
            join(otherTable, joinType, onColumn, otherColumn, additionalConstraint)
        } else {
            join(otherTable, joinType, additionalConstraint)
        }
        joinParts.addAll(new.joinParts)
    }

    class JoinPart (val joinType: JoinType, val table: Table, val pkColumn: Expression<*>, val fkColumn: Expression<*>, val additionalConstraint: (SqlExpressionBuilder.()->Op<Boolean>)? = null) {
    }

    val joinParts: ArrayList<JoinPart> = ArrayList();

    fun innerJoin (otherTable: Table) : Join {
        return join(otherTable, JoinType.INNER)
    }

    fun leftJoin (otherTable: Table) : Join {
        return join(otherTable, JoinType.LEFT)
    }

    fun join (otherTable: Table, joinType: JoinType = JoinType.INNER, additionalConstraint: (SqlExpressionBuilder.()->Op<Boolean>)? = null) : Join {
        val keysPair = findKeys (this, otherTable) ?: findKeys (otherTable, this)
        if (keysPair == null) error ("Cannot join with ${otherTable.tableName} as there is no matching primary key/ foreign key pair")

        return join(otherTable, joinType, keysPair.first, keysPair.second, additionalConstraint)
    }

    fun join(otherTable: Table, joinType: JoinType, onColumn: Expression<*>, otherColumn: Expression<*>, additionalConstraint: (SqlExpressionBuilder.()->Op<Boolean>)? = null): Join {
        val newJoin = Join(table)
        newJoin.joinParts.addAll(joinParts)
        newJoin.joinParts.add(JoinPart(joinType, otherTable, onColumn, otherColumn, additionalConstraint))
        return newJoin
    }

    private fun findKeys(a: ColumnSet, b: ColumnSet): Pair<Column<*>, Column<*>>? {
        for (a_pk in a.columns) {
            val b_fk = b.columns.firstOrNull { it.referee == a_pk }
            if (b_fk != null)
                return a_pk to b_fk
        }
        return null
    }

    override fun describe(s: Session): String {
        val sb = StringBuilder()
        sb.append(table.describe(s))
        for (p in joinParts) {
            sb.append(" ${p.joinType} JOIN ${p.table.describe(s)} ON ${p.pkColumn.toSQL(QueryBuilder(false))} = ${p.fkColumn.toSQL(QueryBuilder(false))}" )
            if (p.additionalConstraint != null)
                sb.append(" and (${SqlExpressionBuilder.(p.additionalConstraint)().toSQL(QueryBuilder(false))})")
        }
        return sb.toString()
    }

    override val columns: List<Column<*>> get() {
        val answer = ArrayList<Column<*>>()
        answer.addAll(table.columns)
        for (p in joinParts)
            answer.addAll(p.table.columns)
        return answer
    }
}

open class Table(name: String = ""): ColumnSet(), DdlAware {
    val tableName = if (name.length() > 0) name else this.javaClass.simpleName.removeSuffix("Table")

    override val columns = ArrayList<Column<*>>()
    override fun describe(s: Session): String = s.identity(this)

    val primaryKeys  = ArrayList<Column<*>>()
    val indices = ArrayList<Pair<Array<out Column<*>>, Boolean>>()

    override val fields: List<Expression<*>>
        get() = columns

    private fun<TColumn: Column<*>> replaceColumn (oldColumn: Column<*>, newColumn: TColumn) : TColumn {
        columns.remove(oldColumn)
        columns.add(newColumn)
        return newColumn
    }

    fun <T> Column<T>.primaryKey(): Column<T> {
        val answer = replaceColumn (this, PKColumn<T>(table, name, columnType))
        primaryKeys.add(answer)
        return answer
    }

    fun <T:Enum<T>> enumeration(name: String, klass: Class<T>) : Column<T> {
        val answer = Column<T>(this, name, EnumerationColumnType(klass))
        columns.add(answer)
        return answer
    }

    fun entityId(name: String, table: IdTable) : Column<EntityID> {
        val answer = Column<EntityID>(this, name, EntityIDColumnType(table))
        columns.add(answer)
        return answer
    }

    fun integer(name: String): Column<Int> {
        val answer = Column<Int>(this, name, IntegerColumnType())
        columns.add(answer)
        return answer
    }

    fun short(name: String): Column<Short> {
        val answer = Column<Short>(this, name, ShortColumnType())
        columns.add(answer)
        return answer
    }

    fun char(name: String): Column<Char> {
        val answer = Column<Char>(this, name, CharacterColumnType())
        columns.add(answer)
        return answer
    }

    fun decimal(name: String, scale: Int, precision: Int): Column<BigDecimal> {
        val answer = Column<BigDecimal>(this, name, DecimalColumnType(scale, precision))
        columns.add(answer)
        return answer
    }

    fun float(name: String): Column<Float> {
        val answer = Column<Float>(this, name, FloatColumnType())
        columns.add(answer)
        return answer
    }

    fun long(name: String): Column<Long> {
        val answer = Column<Long>(this, name, LongColumnType())
        columns.add(answer)
        return answer
    }

    fun date(name: String): Column<DateTime> {
        val answer = Column<DateTime>(this, name, DateColumnType(false))
        columns.add(answer)
        return answer
    }

    fun bool(name: String): Column<Boolean> {
        val answer = Column<Boolean>(this, name, BooleanColumnType())
        columns.add(answer)
        return answer
    }

    fun datetime(name: String): Column<DateTime> {
        val answer = Column<DateTime>(this, name, DateColumnType(true))
        columns.add(answer)
        return answer
    }

    fun timestamp(name: String): Column<DateTime> {
        val answer = Column<DateTime>(this, name, TimestampColumnType())
        columns.add(answer)
        return answer
    }

    fun blob(name: String): Column<Blob> {
        val answer = Column<Blob>(this, name, BlobColumnType())
        columns.add(answer)
        return answer
    }

    fun text(name: String): Column<String> {
        val answer = Column<String>(this, name, StringColumnType())
        columns.add(answer)
        return answer
    }

    fun varchar(name: String, length: Int, collate: String? = null): Column<String> {
        val answer = Column<String>(this, name, StringColumnType(length, collate))
        columns.add(answer)
        return answer
    }

    fun <C:Column<out Number>> C.autoIncrement(): C {
        columnType.isAutoIncrement = true
        return this
    }

    fun <C:Column<EntityID>> C.autoinc(): C {
        columnType.isAutoIncrement = true
        return this
    }

    fun <T, S: T, C:Column<S>> C.references(ref: Column<T>): C {
        referee = ref
        return this
    }

    fun reference(name: String, foreign: IdTable, onDelete: ReferenceOption? = null): Column<EntityID> {
        val column = entityId(name, foreign) references foreign.id
        column.onDelete = onDelete
        return column
    }

    fun<T> Table.reference(name: String, pkColumn: Column<T>): Column<T> {
        val column = Column<T>(this, name, pkColumn.columnType) references pkColumn
        this.columns.add(column)
        return column
    }

    fun optReference(name: String, foreign: IdTable, onDelete: ReferenceOption? = null): Column<EntityID?> {
        val column = reference(name, foreign).nullable()
        column.onDelete = onDelete
        return column
    }

    fun <T:Any> Column<T>.nullable(): Column<T?> {
        val newColumn = Column<T?> (table, name, columnType)
        newColumn.referee = referee
        newColumn.defaultValue = defaultValue
        newColumn.columnType.isNullable = true
        return replaceColumn (this, newColumn)
    }

    fun <T:Any> Column<T>.default(defaultValue: T): Column<T> {
        this.defaultValue = defaultValue
        return this
    }

    fun index (isUnique: Boolean = false, vararg columns: Column<*>) {
        indices.add(columns to isUnique)
    }

    fun<T> Column<T>.index(isUnique: Boolean = false) : Column<T> {
        this.table.index(isUnique, this)
        return this
    }

    fun<T> Column<T>.uniqueIndex() : Column<T> {
        return this.index(true)
    }

    val ddl: String
        get() = createStatement()

    override fun createStatement(): String {
        var ddl = StringBuilder("CREATE TABLE IF NOT EXISTS ${Session.get().identity(this)}")
        if (columns.isNotEmpty()) {
            ddl.append(" (")
            var c = 0;
            for (column in columns) {
                ddl.append(column.descriptionDdl())
                c++
                if (c < columns.size()) {
                    ddl.append(", ")
                }
            }

            ddl.append(")")
        }
        return ddl.toString()
    }

    override fun dropStatement(): String = "DROP TABLE ${Session.get().identity(this)}"

    override fun modifyStatement(): String {
        throw UnsupportedOperationException("Use modify on columns and indices")
    }

    override fun equals(other: Any?): Boolean {
        if (other !is Table) return false
        return  other.tableName == tableName
    }
}

/**
 * Helper class for creating tables that contain objects which are retrieved fully.
 * The formatted class must have a constructor that takes one instance of each table column.
 *
 * class Images: BaseLookupTable<Int, Image>("image_id", Table::integer) {
 *    val url = varchar("image_url", 200)
 *
 *    override fun toData(r: ResultRow) = Image(r[Images.id], r[Images.url])
 * }
 */
open class BaseLookupTable<T: Number, U>(private val type: KClass<*>, keyName: String, keyType: Table.(String) -> Column<T>): Table() {
    val id = keyType(keyName).primaryKey().autoIncrement()

    // The cached constructor we use to create value instances.
    private var constructor: KFunction<U>? = null

    // A set of constructor argument indices that are primitives, and where the corresponding table column can be null.
    private var primitives = ArrayList<Pair<Int, Any>>()

    // A set of constructor argument indices that are strings, and where the corresponding table column can be null.
    private var strings = ArrayList<Int>()

    fun lookup(db: Database, key: T) = find(db) {{id eq key}}
    fun lookupList(db: Database, keys: List<T>) = findList(db) {{id inList keys}}

    open fun format(r: ResultRow): U {
        val values = r.getAll(columns)
        val constructor = findConstructor(values)

        primitives.forEach {values[it.first] = values[it.first] ?: it.second}
        strings.forEach {values[it] = values[it] ?: ""}

        return constructor.call(*values)
    }

    private fun findConstructor(r: Array<Any?>): KFunction<U> {
        // We need a result row to find the correct constructor, because the types from the table are erased.
        if(constructor == null) {
            var errorString = ""

            // Find the correct constructor.
            val constructor = type.constructors.find {
                errorString += "Checking constructor ${it.name}...\n"

                if(it.parameters.size() == r.size()) {
                    // Check if each argument is compatible with the database,
                    // and give a detailed error if not to aid debugging.
                    r.zip(it.javaConstructor!!.parameterTypes).fold(true) {v, p ->
                        val (column, arg) = p
                        // Check if the types are compatible.
                        // We can't really do anything useful if the source is null, though.
                        if(column != null && !arg.isAssignableFrom(column.javaClass) && !arg.isPrimitive) {
                            errorString += "    failed because ${arg.name} is incompatible with ${column.javaClass.name}.\n"
                            false
                        } else true
                    }
                } else {
                    errorString += "    failed because the number of arguments differs.\n"
                    false
                }
            }

            constructor ?: throw IllegalArgumentException("Class ${type.simpleName} has no valid constructor:\n$errorString")

            primitives.clear()
            strings.clear()

            // Find the primitive and string values that need special handling.
            columns.zip(constructor.javaConstructor!!.parameterTypes).forEachIndexed {i, v ->
                if(v.first.columnType.isNullable && v.second.isPrimitive) {
                    primitives.add(Pair(i, when(v.second) {
                        lang.Long.TYPE -> 0L
                        lang.Integer.TYPE -> 0
                        lang.Short.TYPE -> 0.toShort()
                        lang.Byte.TYPE -> 0.toByte()
                        lang.Float.TYPE -> 0f
                        lang.Double.TYPE -> 0.0
                        lang.Boolean.TYPE -> false
                        else -> 0
                    }))
                }
            }

            columns.forEachIndexed {i, column ->
                if(column.columnType.isNullable
                    && !constructor.parameters[i].type.isMarkedNullable
                    && constructor.javaConstructor!!.parameterTypes[i] == String::class.java) {
                    strings.add(i)
                }
            }

            this.constructor = constructor as KFunction<U>
        }

        return constructor!!
    }
}

/**
 * The default key format for lookup tables is Long.
 */
open class LookupTable<T>(type: KClass<*>, keyName: String): BaseLookupTable<Long, T>(type, keyName, Table::long) {}


// Helper functions for creating selects.
inline fun <T, U, V: BaseLookupTable<T, U>> V.find(db: Database, crossinline predicate: V.() -> (SqlExpressionBuilder.() -> Op<Boolean>)) = db.withSession {
    format(select(predicate()).first())
}

inline fun <T, U, V: BaseLookupTable<T, U>> V.findList(db: Database, crossinline predicate: V.() -> (SqlExpressionBuilder.() -> Op<Boolean>)) = db.withSession {
    select(predicate()) map {format(it)}
}
