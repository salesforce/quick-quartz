/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import com.google.common.truth.Truth.assertThat
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.StdOutSqlLogger
import org.jetbrains.exposed.sql.addLogger
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import javax.sql.DataSource

@SpringBootTest
class ExposedTests {
    private val prefix = ExposedTests::class.java.simpleName

    @Autowired
    lateinit var db: DataSource

    @BeforeAll
    fun connect() {
        Database.connect(db)
    }

    @Test
    fun `bulk insert job details`() {
        // cook up some objects to insert
        val inputJobs = mutableListOf<JobDetail>().apply {
            for (i in 1..5) add(
                JobDetail(
                    "$prefix-$i", "$i", "$i", "$i", "$i",
                    isDurable = false, isNonConcurrent = false, isUpdateData = false, requestsRecovery = false
                )
            )
        }

        // insert them in one batch and commit
        transaction {
            addLogger(StdOutSqlLogger)

            inputJobs.batchInsert()
        }

        // read back from another transaction
        transaction {
            addLogger(StdOutSqlLogger)
            val inserted = QuickQuartzJobDetails.select {
                QuickQuartzJobDetails.schedName like "$prefix-%"
            }.map { it.toJobDetails() }

            assertThat(inserted.size).isEqualTo(5)
            assertThat(inserted.map { it.schedName }).isEqualTo(inputJobs.map { it.schedName })
        }
    }
}
