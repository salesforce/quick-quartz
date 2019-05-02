/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import com.google.common.truth.Truth.assertThat
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.MoreExecutors
import com.zaxxer.hikari.HikariConfig
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
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
    fun `check db version and non-ANSI features`() {
        db.connection.use { conn ->
            conn.createStatement().use { stmt ->
                // verify that we are using postgres 11.2
                stmt.executeQuery("select version()").use { rs ->
                    val list = rs.toResultsList { getString(1) }
                    assertThat(list[0]).contains("PostgreSQL 11.2")
                }

                // verify pg syntax works
                stmt.executeQuery("select bar from foo where bar = 'from liquibase' FOR UPDATE SKIP LOCKED").use { rs ->
                    val list = rs.toResultsList { getString("bar") }
                    assertThat(list.size).isEqualTo(1)
                    assertThat(list[0]).isEqualTo("from liquibase")
                }
            }
        }
    }

    /**
     * check that QuickQuartzDb fails if you hand it a datasource with autocommit on.
     */
    @Test
    fun `assert autocommit is off`() {
        // given
        val autocommitting = HikariConfig()
        (db as HikariConfig).copyState(autocommitting)
        autocommitting.isAutoCommit = true

        // QQDb should gack
        val error = assertThrows<Error> { QuickQuartzDb(autocommitting.dataSource) }
        assertThat(error.message).isEqualTo("please turn off autocommit")
    }

    /**
     * check that row level locks work, and that connections who wish to skip past locked rows are able to do so.
     */
    @Test
    fun `assert non blocking row locks`() {
        val service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2))
        val futures = mutableListOf<ListenableFuture<List<String>>>()
        val latch = CountDownLatch(2) // one per thread

        // only one thread should be able to win the row level lock
        // the other thread should return immediately
        for (i in 1..2) {
            futures.add(service.submit<List<String>> {
                db.connection.use { conn ->
                    val list = conn.prepareStatement("select * from foo where bar = 'bar' for update skip locked")
                            .executeQuery().use { rs ->
                                rs.toResultsList { getString(1) }
                            }
                    latch.countDown()
                    latch.await(5, TimeUnit.SECONDS) // wait for the other thread before rolling back
                    conn.rollback()
                    list
                }
            })
        }
        service.shutdown()

        val successful = Futures.successfulAsList(futures).get()
        assertThat(successful.size).isEqualTo(2)

        val first = successful[0]
        val second = successful[1]
        assertThat((first.isEmpty() && second.size == 1).xor(first.size == 1 && second.isEmpty()))
    }

    @ParameterizedTest
    @MethodSource("tables")
    fun `check quartz tables got created by liquibase`(table: String) {
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
