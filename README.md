@Author
Niharika Rajaram - nxr190029

******************************************************************
____Steps to compile and run the program____
Extract Team_Dubai.zip and change directory to the folder

Run the following code:

javac DavisBase.java
java DavisBase

*******************************************************************

Supported Commands:

All commands below are case insensitive

SHOW TABLES;
        Display the names of all tables.

CREATE TABLE <table_name> (<column_name> <data_type> <not_null> <unique>);
        Creates a table with the given columns.

CREATE INDEX ON <table_name> (<column_name>);
        Creates an index on a column in the table.

DROP TABLE <table_name>;
        Remove table data (i.e. all records) and its schema.

UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];
        Modify records data whose optional <condition>
        is <column_name> = <value>.

INSERT INTO <table_name> (<column_list>) VALUES (<values_list>);
        Inserts a new record into the table with the given values for the given columns.

DELETE FROM TABLE <table_name> [WHERE <condition>];
        Delete one or more table records whose optional <condition>
        is <column_name> = <value>.

SELECT <column_list> FROM <table_name> [WHERE <condition>];
        Display table records whose optional <condition>
        is <column_name> = <value>.

VERSION;
        Display the program version.

HELP;
        Display this help information.

EXIT;
        Exit the program.
