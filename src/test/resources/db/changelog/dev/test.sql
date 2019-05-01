DROP TABLE IF EXISTS  foo;

CREATE TABLE foo
(
    bar VARCHAR NOT NULL,
    PRIMARY KEY (bar)
);

insert into foo values ('from liquibase');
insert into foo values ('bar');