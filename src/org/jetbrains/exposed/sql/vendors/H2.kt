package org.jetbrains.exposed.sql.vendors

import org.jetbrains.exposed.sql.Session

/**
 * User: Andrey.Tarashevskiy
 * Date: 05.10.2015
 */

internal object H2Dialect: VendorDialect() {

    // h2 supports only JDBC API from Java 1.6
    override fun getDatabase(): String {
        return Session.get().connection.catalog
    }
}
