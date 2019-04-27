/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.liquibase.LiquibaseDataSource
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
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
        val config = HikariConfig()
        config.jdbcUrl = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE"
        config.username = "sa"
        config.password = ""
        config.driverClassName = "org.h2.Driver"
        config.maximumPoolSize = 4
        config.poolName = "testdb"

        return HikariDataSource(config)
    }

    @Bean
    fun sample(): SampleBean {
        return SampleBean("zero")
    }
}

data class SampleBean(val name: String)
