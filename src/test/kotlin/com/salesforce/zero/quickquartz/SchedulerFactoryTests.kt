/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import com.google.common.truth.Truth.assertThat
import org.junit.jupiter.api.Test
import org.quartz.impl.StdSchedulerFactory
import java.util.Properties

class SchedulerFactoryTests {
    private val properties: Properties
        get() {
            val quartzProps = Properties()
            quartzProps["org.quartz.jobStore.class"] = "com.salesforce.zero.quickquartz.QuickQuartz"

            quartzProps["org.quartz.threadPool.threadCount"] = "2"
            quartzProps["org.quartz.scheduler.batchTriggerAcquisitionMaxCount"] = "2"
            quartzProps["org.quartz.scheduler.instanceId"] = "AUTO"
            return quartzProps
        }

    @Test
    fun standardSchedulerFactory() {
        val schedulerFactory = StdSchedulerFactory(properties)
        val scheduler = schedulerFactory.scheduler
        assertThat(scheduler).isNotNull()
    }
}
