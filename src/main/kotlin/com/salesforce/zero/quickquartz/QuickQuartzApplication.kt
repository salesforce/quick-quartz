package com.salesforce.zero.quickquartz

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class QuickQuartzApplication

fun main(args: Array<String>) {
	runApplication<QuickQuartzApplication>(*args)
}
