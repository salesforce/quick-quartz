SETTINGS_FILE ?= settings.xml

build:
	mvn spotless:check
	mvn -s ${SETTINGS_FILE} package

spotless-apply:
	mvn spotless:apply

test:
	mvn -s ${SETTINGS_FILE} test jacoco:report

view-codecov-report:
	open target/site/jacoco/index.html
