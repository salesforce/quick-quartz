SETTINGS_FILE ?= settings.xml

build:
	mvn -s ${SETTINGS_FILE} spotless:check
	mvn -s ${SETTINGS_FILE} package

spotless-apply:
	mvn -s ${SETTINGS_FILE} spotless:apply

test:
	mvn -s ${SETTINGS_FILE} test jacoco:report

view-codecov-report:
	open target/site/jacoco/index.html

version-display-updates:
	mvn -s ${SETTINGS_FILE} versions:display-dependency-updates
