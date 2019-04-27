/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import com.google.common.truth.Truth.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.sql.ResultSet
import javax.sql.DataSource

/**
 * tests can use spring to bootstrap an in-memory database with liquibase
 */
@SpringBootTest
class H2Tests {

    @Autowired lateinit var db: DataSource
    @Autowired lateinit var sample: SampleBean

    @Test
    fun testBean() {
        assertThat(sample.name).isEqualTo("zero")
    }

    @Test
    fun h2() {
        db.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("select bar from foo").use { rs ->
                    val list = rs.toResultsList { getString("bar") }
                    assertThat(list.size).isEqualTo(1)
                    assertThat(list[0]).isEqualTo("from liquibase")
                }
            }
        }
    }
}

/**
 * a handy extension function that applies the given lambda fn to each row in the resultSet and transforms it into a list
 * think: Iterables.transform() except that ResultSet is not an iterable :)
 */
inline fun <T> ResultSet.toResultsList(fn: ResultSet.() -> T): List<T> =
    mutableListOf<T>().apply {
        while (next()) {
            add(fn(this@toResultsList))
        }
    }
