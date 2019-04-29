/* Copyright (c) 2019, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause.
 * For full license text, see LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause.
 */
package com.salesforce.zero.quickquartz

import com.google.common.truth.Truth.assertThat
import org.junit.jupiter.api.Test
import org.quartz.Scheduler
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest
class EndToEndTests {

    @Autowired lateinit var scheduler: Scheduler

    @Test
    fun useScheduler() {
        assertThat(scheduler.schedulerName).isEqualTo("QuickQuartzScheduler")
    }
}