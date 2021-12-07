import java.io.File;
import java.util.Scanner;

import static java.lang.System.out;

/**
 * @author Team Dubai
 * @version 1.0 <b>
 * <p>
 * This is an example of how to create an interactive prompt
 * </p>
 * <p>
 * There is also some guidance to get started with read/write of binary
 * data files using RandomAccessFile class
 * </p>
 * </b>
 */
public class DavisBase {

    static String prompt = "DavisBase> ";
    static String version = "v1.0";
    static String copyright = "* HexDump (c)2018 Chris Irwin Davis";

    static boolean isExit = false;

    /*
     * The Scanner class is used to collect user commands from the prompt There are
     * many ways to do this. This is just one.
     *
     * Each time the semicolon (;) delimiter is entered, the userCommand String is
     * re-populated.
     */
    static Scanner scanner = new Scanner(System.in).useDelimiter(";");

    /**
     * ******** Main method ******************
     */
    public static void main(String[] args) {

        /* Display the welcome screen */
        splashScreen();

        File dataDir = new File("data");

        if (!new File(dataDir, DavisBaseBinaryFile.tablesTable + ".tbl").exists()
                || !new File(dataDir, DavisBaseBinaryFile.columnsTable + ".tbl").exists())
            DavisBaseBinaryFile.initializeDataStore();
        else
            DavisBaseBinaryFile.dataStoreInitialized = true;

        /* Variable to collect user input from the prompt */
        String userCommand;

        while (!isExit) {
            System.out.print(prompt);
            /* toLowerCase() renders command case insensitive */
            userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
            // userCommand = userCommand.replace("\n", "").replace("\r", "");
            Command.parseUserCommand(userCommand);
        }
        System.out.println();
    }

    /**
     * Display the splash screen
     */
    public static void splashScreen() {
        System.out.println(line("-", 80));
        System.out.println("Welcome to DavisBase"); // Display the string.
        System.out.println("DavisBase Version " + getVersion());
        System.out.println(getCopyright());
        System.out.println("\nType \"help;\" to display supported commands.");
        System.out.println(line("-", 80));
    }

    /**
     * @param s   The String to be repeated
     * @param num The number of time to repeat String s.
     * @return String A String object, which is the String s appended to itself num
     * times.
     */
    public static String line(String s, int num) {
        return String.valueOf(s).repeat(Math.max(0, num));
    }

    /**
     * Help: Display supported commands
     */
    public static void help() {

        out.println("SUPPORTED COMMANDS\n");
        out.println("All commands below are case insensitive\n");

        out.println("SHOW TABLES;");
        out.println("\tDisplay the names of all tables.\n");

        out.println("CREATE TABLE <table_name> (<column_name> <data_type> <not_null> <unique>);");
        out.println("\tCreates a table with the given columns.\n");

        out.println("CREATE INDEX ON <table_name> (<column_name>);");
        out.println("\tCreates an index on a column in the table. \n");

        out.println("DROP TABLE <table_name>;");
        out.println("\tRemove table data (i.e. all records) and its schema.\n");

        out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
        out.println("\tModify records data whose optional <condition>");
        out.println("\tis <column_name> = <value>.\n");

        out.println("INSERT INTO <table_name> (<column_list>) VALUES (<values_list>);");
        out.println("\tInserts a new record into the table with the given values for the given columns.\n");

        out.println("DELETE FROM TABLE <table_name> [WHERE <condition>];");
        out.println("\tDelete one or more table records whose optional <condition>");
        out.println("\tis <column_name> = <value>.\n");

        out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
        out.println("\tDisplay table records whose optional <condition>");
        out.println("\tis <column_name> = <value>.\n");

        out.println("VERSION;");
        out.println("\tDisplay the program version.\n");

        out.println("HELP;");
        out.println("\tDisplay this help information.\n");

        out.println("EXIT;");
        out.println("\tExit the program.\n");

        out.println(line("*", 80));
    }

    /**
     * return the DavisBase version
     */
    public static String getVersion() {
        return version;
    }

    public static String getCopyright() {
        return copyright;
    }

    public static void displayVersion() {
        System.out.println("DavisBase Version " + getVersion());
        System.out.println(getCopyright());
    }

    public static String getTBLFilePath(String tableName) {
        return "data/" + tableName + ".tbl";
    }

    public static String getNDXFilePath(String tableName, String columnName) {
        return "data/" + tableName + "_" + columnName + ".ndx";
    }

}