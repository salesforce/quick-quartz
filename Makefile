build:
	mvn spotless:check
	mvn -s settings.xml package

spotless-apply:
	mvn spotless:apply
