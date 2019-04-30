[![Build Status](https://travis-ci.org/salesforce/quick-quartz.svg?branch=master)](https://travis-ci.org/salesforce/quick-quartz)
[![codecov](https://codecov.io/gh/salesforce/quick-quartz/branch/master/graph/badge.svg)](https://codecov.io/gh/salesforce/quick-quartz)

# quick-quartz

A concurrent JDBC job store implementation for [Quartz](https://github.com/quartz-scheduler/quartz).

## build 
```sh
make
```

## quick start

Set the job store in your quartz properties: 
```sh
org.quartz.jobStore.class=com.salesforce.zero.quickquartz.QuickQuartz
```

Use `QuickQuartzSchedulerFactory` to create a `Scheduler`:
```kotlin
@Bean
fun scheduler(): Scheduler {
    return QuickQuartzSchedulerFactory(myDataSource(), myQuartzProperties).scheduler
}
``` 

Then use the scheduler as you normally would. 
