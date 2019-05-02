/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import com.opentable.db.postgres.embedded.PgBinaryResolver
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.quartz.Scheduler
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.liquibase.LiquibaseDataSource
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.core.io.ClassPathResource
import java.io.InputStream
import java.util.Properties
import javax.sql.DataSource

/**
 * boot up a spring application context for tests.
 * note that individual tests can just as easily define their own app/configs.
 */
@SpringBootApplication
@TestConfiguration
class TestConfig {

    @LiquibaseDataSource
    @Bean
    fun testDb(): DataSource {
        val pg = EmbeddedPostgres.builder()
            .setServerConfig("timezone", "GMT")
            .setPgBinaryResolver(Pg11BinaryResolver())
            .start()

        val config = HikariConfig()
        config.dataSource = pg.postgresDatabase
        config.driverClassName = "org.postgresql.Driver"
        config.maximumPoolSize = 4
        config.poolName = "testdb"
        config.isAutoCommit = false
        return HikariDataSource(config)
    }

    private val properties: Properties
        get() {
            val quartzProps = Properties()
            quartzProps["org.quartz.jobStore.class"] = "com.salesforce.zero.quickquartz.QuickQuartz"

            quartzProps["org.quartz.threadPool.threadCount"] = "2"
            quartzProps["org.quartz.scheduler.batchTriggerAcquisitionMaxCount"] = "2"
            quartzProps["org.quartz.scheduler.instanceId"] = "AUTO"
            return quartzProps
        }

    @Bean
    fun scheduler(): Scheduler {
        return QuickQuartzSchedulerFactory(testDb(), properties).scheduler
    }

    @Bean
    fun sample(): SampleBean {
        return SampleBean("zero")
    }
}

data class SampleBean(val name: String)

class Pg11BinaryResolver : PgBinaryResolver {
    override fun getPgBinary(system: String?, machineHardware: String?): InputStream =
        ClassPathResource("pg11-$system-$machineHardware.txz").inputStream
}
