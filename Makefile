build:
	mvn spotless:check
	mvn package

spotless-apply:
	mvn spotless:apply
