build:
	mvn spotless:check
	mvn -s settings.xml package

spotless-apply:
	mvn spotless:apply

test:
	mvn -s settings.xml test jacoco:report

view-codecov-report:
	open target/site/jacoco/index.html
