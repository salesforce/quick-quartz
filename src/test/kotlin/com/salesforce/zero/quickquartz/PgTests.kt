/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import com.google.common.truth.Truth.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import javax.sql.DataSource

/**
 * tests can use spring to bootstrap a transient database with liquibase
 */
@SpringBootTest
class PgTests {

    @Autowired lateinit var db: DataSource
    @Autowired lateinit var sample: SampleBean

    @Test
    fun testBean() {
        assertThat(sample.name).isEqualTo("zero")
    }

    @Test
    fun pg() {
        db.connection.use { conn ->
            conn.createStatement().use { stmt ->
                // verify that we are using postgres 10.6
                stmt.executeQuery("select version()").use { rs ->
                    val list = rs.toResultsList { getString(1) }
                    assertThat(list[0]).contains("PostgreSQL 10.6")
                }

                // verify pg syntax works
                stmt.executeQuery("select bar from foo FOR UPDATE SKIP LOCKED").use { rs ->
                    val list = rs.toResultsList { getString("bar") }
                    assertThat(list.size).isEqualTo(1)
                    assertThat(list[0]).isEqualTo("from liquibase")
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("tables")
    fun tablesExist(table: String) {
        println("asked to look at table $table")
        db.connection.use { conn ->
            conn.prepareStatement("select count(*) from pg_stat_user_tables where relname = ?").apply {
                setString(1, table)
            }.executeQuery().use { rs ->
                val list = rs.toResultsList { getInt(1) }
                assertThat(list.size).isEqualTo(1)
                assertThat(list[0]).isEqualTo(1)
            }
        }
    }

    companion object {
        @JvmStatic
        fun tables() = arrayOf(
            Arguments.of("qrtz_job_details"),
            Arguments.of("qrtz_triggers")
        )
    }
}
