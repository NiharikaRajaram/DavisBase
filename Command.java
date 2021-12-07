import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.lang.System.out;

public class Command {
//    public static void test() {
//        Scanner scan = new Scanner(System.in);
//        DavisBasePrompt.parseUserCommand("create table test (id int, name text)");
//        scan.nextLine();
//        DavisBasePrompt.parseUserCommand("create index on test (name)");
//        scan.nextLine();
//        for (int i = 1; i < 35; i++) {
//            //   System.out.println(i);
//            DavisBasePrompt.parseUserCommand("insert into test (id , name) values (" + (i) + ", " + i + "'arun' )");
//
//            //scan.nextLine();
//        }
//        DavisBasePrompt.parseUserCommand("show tables");
//
//        scan.nextLine();
//
//    }

    public static void parseCreateIndex(String createIndexString) {
        ArrayList<String> createIndexTokens = new ArrayList<>(Arrays.asList(createIndexString.split(" ")));
        try {
            if (!createIndexTokens.get(2).equals("on") || !createIndexString.contains("(")
                    || !createIndexString.contains(")") && createIndexTokens.size() < 4) {
                System.out.println("Error in Syntax.");
                return;
            }

            String tableName = createIndexString
                    .substring(createIndexString.indexOf("on") + 3, createIndexString.indexOf("(")).trim();
            String columnName = createIndexString
                    .substring(createIndexString.indexOf("(") + 1, createIndexString.indexOf(")")).trim();

            // check if the index already exists
            if (new File(DavisBase.getNDXFilePath(tableName, columnName)).exists()) {
                System.out.println("Index already exists");
                return;
            }

            RandomAccessFile tableFile = new RandomAccessFile(DavisBase.getTBLFilePath(tableName), "rw");

            TableMetaData metaData = new TableMetaData(tableName);

            if (!metaData.tableExists) {
                System.out.println("Invalid Table name");
                tableFile.close();
                return;
            }

            int columnOrdinal = metaData.columnNames.indexOf(columnName);

            if (columnOrdinal < 0) {
                System.out.println("Invalid column name");
                tableFile.close();
                return;
            }


            // create index file
            RandomAccessFile indexFile = new RandomAccessFile(DavisBase.getNDXFilePath(tableName, columnName), "rw");
            Page.addNewPage(indexFile, Page.PageType.LEAFINDEX, -1, -1);


            if (metaData.recordCount > 0) {
                BPlusOneTree bPlusOneTree = new BPlusOneTree(tableFile, metaData.rootPageNo, metaData.tableName);
                for (int pageNo : bPlusOneTree.getAllLeaves()) {
                    Page page = new Page(tableFile, pageNo);
                    BTree bTree = new BTree(indexFile);
                    for (TableRecord record : page.getPageRecords()) {
                        bTree.insert(record.getAttributes().get(columnOrdinal), record.rowId);
                    }
                }
            }

            System.out.println("Index has been created on the column : " + columnName);
            indexFile.close();
            tableFile.close();

        } catch (IOException e) {

            System.out.println("Error on creating Index");
            System.out.println(e);
        }

    }

    /**
     * Stub method for dropping tables
     *
     * @param dropTableString is a String of the user input
     */
    public static void dropTable(String dropTableString) {
        // System.out.println("STUB: This is the dropTable method.");
        // System.out.println("\tParsing the string:\"" + dropTableString + "\"");

        String[] tokens = dropTableString.split(" ");
        if (!(tokens[0].trim().equalsIgnoreCase("DROP") && tokens[1].trim().equalsIgnoreCase("TABLE"))) {
            System.out.println("Error");
            return;
        }

        ArrayList<String> dropTableTokens = new ArrayList<>(Arrays.asList(dropTableString.split(" ")));
        String tableName = dropTableTokens.get(2);


        parseDelete("delete from table " + DavisBaseBinaryFile.tablesTable + " where table_name = '" + tableName + "' ");
        parseDelete("delete from table " + DavisBaseBinaryFile.columnsTable + " where table_name = '" + tableName + "' ");
        File tableFile = new File("data/" + tableName + ".tbl");
        if (tableFile.delete()) {
            System.out.println("Table " + tableName + " deleted");
        } else System.out.println("table doesn't exist");


        File f = new File("data/");
        File[] matchingFiles = f.listFiles((dir, name) -> name.startsWith(tableName) && name.endsWith("ndx"));
        boolean iFlag = false;
        assert matchingFiles != null;
        for (File file : matchingFiles) {
            if (file.delete()) {
                iFlag = true;
                System.out.println("index deleted");
            }
        }
        if (iFlag)
            System.out.println("drop " + tableName);
        else
            System.out.println("Index doesn't exist");


        //page.DeleteTableRecord(dropTableTokens.get(1) ,record.pageHeaderIndex);
    }

    public static Condition extractConditionFromQuery(TableMetaData tableMetaData, String query) throws Exception {
        if (query.contains("where")) {
            Condition condition = new Condition(DataType.TEXT);
            String whereClause = query.substring(query.indexOf("where") + 6);
            ArrayList<String> whereClauseTokens = new ArrayList<>(Arrays.asList(whereClause.split(" ")));

            // WHERE NOT column operator value
            if (whereClauseTokens.get(0).equalsIgnoreCase("not")) {
                condition.setNegation(true);
            }


            for (int i = 0; i < Condition.supportedOperators.length; i++) {
                if (whereClause.contains(Condition.supportedOperators[i])) {
                    whereClauseTokens = new ArrayList<>(
                            Arrays.asList(whereClause.split(Condition.supportedOperators[i])));
                    {
                        condition.setOperator(Condition.supportedOperators[i]);
                        condition.setConditionValue(whereClauseTokens.get(1).trim());
                        condition.setColumName(whereClauseTokens.get(0).trim());
                        break;
                    }

                }
            }


            if (tableMetaData.tableExists
                    && tableMetaData.columnExists(new ArrayList<>(List.of(condition.columnName)))) {
                condition.columnOrdinal = tableMetaData.columnNames.indexOf(condition.columnName);
                condition.dataType = tableMetaData.columnNameAttrs.get(condition.columnOrdinal).dataType;
            } else {
                throw new Exception(
                        "Invalid Table/Column : " + tableMetaData.tableName + " . " + condition.columnName);
            }
            return condition;
        } else
            return null;
    }

    /**
     * Stub method for executing queries
     *
     * @param queryString is a String of the user input
     */
    public static void parseQuery(String queryString) {
        String table_name = "";
        List<String> column_names = new ArrayList<>();

        // Get table and column names for the select
        ArrayList<String> queryTableTokens = new ArrayList<>(Arrays.asList(queryString.split(" ")));
        int i;

        for (i = 1; i < queryTableTokens.size(); i++) {
            if (queryTableTokens.get(i).equals("from")) {
                ++i;
                table_name = queryTableTokens.get(i);
                break;
            }
            if (!queryTableTokens.get(i).equals("*") && !queryTableTokens.get(i).equals(",")) {
                if (queryTableTokens.get(i).contains(",")) {
                    ArrayList<String> colList = new ArrayList<>(
                            Arrays.asList(queryTableTokens.get(i).split(",")));
                    for (String col : colList) {
                        column_names.add(col.trim());
                    }
                } else
                    column_names.add(queryTableTokens.get(i));
            }
        }

        TableMetaData tableMetaData = new TableMetaData(table_name);
        if (!tableMetaData.tableExists) {
            System.out.println("Table " + table_name + " does not exist.");
            return;
        }

        Condition condition;
        try {

            condition = extractConditionFromQuery(tableMetaData, queryString);

        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }

        if (column_names.size() == 0) {
            column_names = tableMetaData.columnNames;
        }
        try {

            RandomAccessFile tableFile = new RandomAccessFile(DavisBase.getTBLFilePath(table_name), "r");
            DavisBaseBinaryFile tableBinaryFile = new DavisBaseBinaryFile(tableFile);
            tableBinaryFile.selectRecords(tableMetaData, column_names, condition);
            tableFile.close();
        } catch (IOException exception) {
            System.out.println("Error while retrieving data.");
        }

    }

    /**
     * Stub method for updating records
     *
     * @param updateString is a String of the user input
     */
    public static void parseUpdate(String updateString) {
        ArrayList<String> updateTokens = new ArrayList<>(Arrays.asList(updateString.split(" ")));

        String table_name = updateTokens.get(1);
        List<String> columnsToUpdate = new ArrayList<>();
        List<String> valueToUpdate = new ArrayList<>();

        if (!updateTokens.get(2).equals("set") || !updateTokens.contains("=")) {
            System.out.println("Error in Syntax.");
            System.out.println(
                    "Expected Syntax: UPDATE [table_name] SET [Column_name] = val1 where [column_name] = val2; ");
            return;
        }

        String updateColInfoString = updateString.split("set")[1].split("where")[0];

        String[] column_newValueSet = updateColInfoString.split(",");

        for (String item : column_newValueSet) {
            columnsToUpdate.add(item.split("=")[0].trim());
            valueToUpdate.add(item.split("=")[1].trim().replace("\"", "").replace("'", ""));
        }

        TableMetaData metadata = new TableMetaData(table_name);

        if (!metadata.tableExists) {
            System.out.println("Invalid Table name");
            return;
        }

        if (!metadata.columnExists(columnsToUpdate)) {
            System.out.println("Invalid column name(s)");
            return;
        }

        Condition condition;
        try {

            condition = extractConditionFromQuery(metadata, updateString);

        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;

        }


        try {
            RandomAccessFile file = new RandomAccessFile(DavisBase.getTBLFilePath(table_name), "rw");
            DavisBaseBinaryFile binaryFile = new DavisBaseBinaryFile(file);
            int noOfRecordsupdated = binaryFile.updateRecords(metadata, condition, columnsToUpdate, valueToUpdate);

            if (noOfRecordsupdated > 0) {
                List<Integer> allRowids = new ArrayList<>();
                for (ColumnInfo colInfo : metadata.columnNameAttrs) {
                    for (int i = 0; i < columnsToUpdate.size(); i++)
                        if (colInfo.columnName.equals(columnsToUpdate.get(i)) && colInfo.hasIndex) {

                            // when there is no condition, All rows in the column gets updated the index value point to all rowids
                            if (condition == null) {
                                //Delete the index file.

                                if (allRowids.size() == 0) {
                                    BPlusOneTree bPlusOneTree = new BPlusOneTree(file, metadata.rootPageNo, metadata.tableName);
                                    for (int pageNo : bPlusOneTree.getAllLeaves()) {
                                        Page currentPage = new Page(file, pageNo);
                                        for (TableRecord record : currentPage.getPageRecords()) {
                                            allRowids.add(record.rowId);
                                        }
                                    }
                                }
                                //create a new index value and insert 1 index value with all rowids
                                RandomAccessFile indexFile = new RandomAccessFile(DavisBase.getNDXFilePath(table_name, columnsToUpdate.get(i)),
                                        "rw");
                                Page.addNewPage(indexFile, Page.PageType.LEAFINDEX, -1, -1);
                                BTree bTree = new BTree(indexFile);
                                bTree.insert(new Attribute(colInfo.dataType, valueToUpdate.get(i)), allRowids);
                            }
                        }
                }
            }

            file.close();

        } catch (Exception e) {
            out.println("Unable to update the " + table_name + " file");
            out.println(e);

        }


    }

    public static void parseInsert(String queryString) {
        // INSERT INTO table_name ( columns ) VALUES ( values );
        ArrayList<String> insertTokens = new ArrayList<>(Arrays.asList(queryString.split(" ")));

        if (!insertTokens.get(1).equals("into") || !queryString.contains(") values")) {
            System.out.println("Error in Syntax.");
            System.out.println("Expected Syntax: INSERT INTO table_name ( columns ) VALUES ( values );");

            return;
        }

        try {
            String tableName = insertTokens.get(2);
            if (tableName.trim().length() == 0) {
                System.out.println("Tablename cannot be empty");
                return;
            }

            // parsing logic
            if (tableName.contains("(")) {
                tableName = tableName.substring(0, tableName.indexOf("("));
            }
            TableMetaData dstMetaData = new TableMetaData(tableName);

            if (!dstMetaData.tableExists) {
                System.out.println("Table does not exist.");
                return;
            }

            ArrayList<String> columnTokens = new ArrayList<>(Arrays.asList(
                    queryString.substring(queryString.indexOf("(") + 1, queryString.indexOf(") values")).split(",")));

            // Column List validation
            for (String colToken : columnTokens) {
                if (!dstMetaData.columnNames.contains(colToken.trim())) {
                    System.out.println("Invalid column : " + colToken.trim());
                    return;
                }
            }

            String valuesString = queryString.substring(queryString.indexOf("values") + 6, queryString.length() - 1);

            ArrayList<String> valueTokens = new ArrayList<>(Arrays
                    .asList(valuesString.substring(valuesString.indexOf("(") + 1).split(",")));

            // fill attributes to insert
            List<Attribute> attributeToInsert = new ArrayList<>();

            for (ColumnInfo colInfo : dstMetaData.columnNameAttrs) {
                int i;
                boolean columnProvided = false;
                for (i = 0; i < columnTokens.size(); i++) {
                    if (columnTokens.get(i).trim().equals(colInfo.columnName)) {
                        columnProvided = true;
                        try {
                            String value = valueTokens.get(i).replace("'", "").replace("\"", "").trim();
                            if (valueTokens.get(i).trim().equals("null")) {
                                if (!colInfo.isNullable) {
                                    System.out.println("Cannot Insert NULL into " + colInfo.columnName);
                                    return;
                                }
                                colInfo.dataType = DataType.NULL;
                                value = value.toUpperCase();
                            }
                            Attribute attr = new Attribute(colInfo.dataType, value);
                            attributeToInsert.add(attr);
                            break;
                        } catch (Exception e) {
                            System.out.println("Invalid data format for " + columnTokens.get(i) + " values: "
                                    + valueTokens.get(i));
                            return;
                        }
                    }
                }
                if (columnTokens.size() > i) {
                    columnTokens.remove(i);
                    valueTokens.remove(i);
                }

                if (!columnProvided) {
                    if (colInfo.isNullable)
                        attributeToInsert.add(new Attribute(DataType.NULL, "NULL"));
                    else {
                        System.out.println("Cannot Insert NULL into " + colInfo.columnName);
                        return;
                    }
                }
            }

            // insert attributes to the page
            RandomAccessFile dstTable = new RandomAccessFile(DavisBase.getTBLFilePath(tableName), "rw");
            int dstPageNo = BPlusOneTree.getPageNoForInsert(dstTable, dstMetaData.rootPageNo);
            Page dstPage = new Page(dstTable, dstPageNo);

            int rowNo = dstPage.addTableRow(tableName, attributeToInsert);

            // update Index
            if (rowNo != -1) {

                for (int i = 0; i < dstMetaData.columnNameAttrs.size(); i++) {
                    ColumnInfo col = dstMetaData.columnNameAttrs.get(i);

                    if (col.hasIndex) {
                        RandomAccessFile indexFile = new RandomAccessFile(DavisBase.getNDXFilePath(tableName, col.columnName),
                                "rw");
                        BTree bTree = new BTree(indexFile);
                        bTree.insert(attributeToInsert.get(i), rowNo);
                    }

                }
            }

            dstTable.close();
            if (rowNo != -1)
                System.out.println("Inserted the record successfully.");
//            System.out.println();

        } catch (Exception ex) {
            System.out.println("Error while inserting record");
            System.out.println(ex);

        }
    }

    /**
     * Create new table
     * <p>
     * is a String of the user input
     */
    public static void parseCreateTable(String createTableString) {

        ArrayList<String> createTableTokens = new ArrayList<>(Arrays.asList(createTableString.split(" ")));
        // table and () check
        if (!createTableTokens.get(1).equals("table")) {
            System.out.println("Error in Syntax.");
            return;
        }
        String tableName = createTableTokens.get(2);
        if (tableName.trim().length() == 0) {
            System.out.println("Tablename cannot be empty");
            return;
        }
        try {

            if (tableName.contains("(")) {
                tableName = tableName.substring(0, tableName.indexOf("("));
            }

            List<ColumnInfo> lstcolumnInformation = new ArrayList<>();
            ArrayList<String> columnTokens = new ArrayList<>(Arrays.asList(createTableString
                    .substring(createTableString.indexOf("(") + 1, createTableString.length() - 1).split(",")));

            short ordinalPosition = 1;

            String primaryKeyColumn = "";

            for (String columnToken : columnTokens) {

                ArrayList<String> colInfoToken = new ArrayList<>(Arrays.asList(columnToken.trim().split(" ")));
                ColumnInfo colInfo = new ColumnInfo();
                colInfo.tableName = tableName;
                colInfo.columnName = colInfoToken.get(0);
                colInfo.isNullable = true;
                colInfo.dataType = DataType.get(colInfoToken.get(1).toUpperCase());
                for (int i = 0; i < colInfoToken.size(); i++) {

                    if ((colInfoToken.get(i).equals("null"))) {
                        colInfo.isNullable = true;
                    }
                    if (colInfoToken.get(i).contains("not") && (colInfoToken.get(i + 1).contains("null"))) {
                        colInfo.isNullable = false;
                        i++;
                    }

                    if ((colInfoToken.get(i).equals("unique"))) {
                        colInfo.isUnique = true;
                    } else if (colInfoToken.get(i).contains("primary") && (colInfoToken.get(i + 1).contains("key"))) {
                        colInfo.isPrimaryKey = true;
                        colInfo.isUnique = true;
                        colInfo.isNullable = false;
                        primaryKeyColumn = colInfo.columnName;
                        i++;
                    }

                }
                colInfo.ordinalPosition = ordinalPosition++;
                lstcolumnInformation.add(colInfo);

            }

            // update sys file
            RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile(
                    DavisBase.getTBLFilePath(DavisBaseBinaryFile.tablesTable), "rw");
            TableMetaData davisbaseTableMetaData = new TableMetaData(DavisBaseBinaryFile.tablesTable);

            int pageNo = BPlusOneTree.getPageNoForInsert(davisbaseTablesCatalog, davisbaseTableMetaData.rootPageNo);

            Page page = new Page(davisbaseTablesCatalog, pageNo);

            int rowNo = page.addTableRow(DavisBaseBinaryFile.tablesTable,
                    Arrays.asList(new Attribute(DataType.TEXT, tableName), // DavisBaseBinaryFile.tablesTable->test
                            new Attribute(DataType.INT, "0"), new Attribute(DataType.SMALLINT, "0"),
                            new Attribute(DataType.SMALLINT, "0")));
            davisbaseTablesCatalog.close();

            if (rowNo == -1) {
                System.out.println("Duplicate table Name");
                return;
            }
            RandomAccessFile tableFile = new RandomAccessFile(DavisBase.getTBLFilePath(tableName), "rw");
            Page.addNewPage(tableFile, Page.PageType.LEAF, -1, -1);
            tableFile.close();

            RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile(
                    DavisBase.getTBLFilePath(DavisBaseBinaryFile.columnsTable), "rw");
            TableMetaData davisbaseColumnsMetaData = new TableMetaData(DavisBaseBinaryFile.columnsTable);
            pageNo = BPlusOneTree.getPageNoForInsert(davisbaseColumnsCatalog, davisbaseColumnsMetaData.rootPageNo);

            Page page1 = new Page(davisbaseColumnsCatalog, pageNo);

            for (ColumnInfo column : lstcolumnInformation) {
                page1.addNewColumn(column);
            }

            davisbaseColumnsCatalog.close();

            System.out.println("Table " + tableName + " has been created.");

            if (primaryKeyColumn.length() > 0) {
                parseCreateIndex("create index on " + tableName + "(" + primaryKeyColumn + ")");
            }
        } catch (Exception e) {

            System.out.println("Error on creating Table");
            System.out.println(e.getMessage());
            parseDelete("delete from table " + DavisBaseBinaryFile.tablesTable + " where table_name = '" + tableName
                    + "' ");
            parseDelete("delete from table " + DavisBaseBinaryFile.columnsTable + " where table_name = '" + tableName
                    + "' ");
        }

    }

    /**
     * Delete records from table
     * <p>
     * is a String of the user input
     */
    public static void parseDelete(String deleteTableString) {
        ArrayList<String> deleteTableTokens = new ArrayList<>(Arrays.asList(deleteTableString.split(" ")));

        String tableName = "";

        try {

            if (!deleteTableTokens.get(1).equals("from") || !deleteTableTokens.get(2).equals("table")) {
                System.out.println("Error in Syntax.");
                return;
            }

            tableName = deleteTableTokens.get(3);

            TableMetaData metaData = new TableMetaData(tableName);
            Condition condition;
            try {
                condition = extractConditionFromQuery(metaData, deleteTableString);

            } catch (Exception e) {
                System.out.println(e);
                return;
            }
            RandomAccessFile tableFile = new RandomAccessFile(DavisBase.getTBLFilePath(tableName), "rw");

            BPlusOneTree tree = new BPlusOneTree(tableFile, metaData.rootPageNo, metaData.tableName);
            List<TableRecord> deletedRecords = new ArrayList<>();
            int count = 0;
            for (int pageNo : tree.getAllLeaves(condition)) {
                short deleteCountPerPage = 0;
                Page page = new Page(tableFile, pageNo);
                for (TableRecord record : page.getPageRecords()) {
                    if (condition != null) {
                        if (!condition.checkCondition(record.getAttributes().get(condition.columnOrdinal).fieldValue))
                            continue;
                    }

                    deletedRecords.add(record);
                    page.DeleteTableRecord(tableName,
                            Integer.valueOf(record.pageHeaderIndex - deleteCountPerPage).shortValue());
                    deleteCountPerPage++;
                    count++;
                }
            }

            // update Index

            // if there is no condition, all the rows will be deleted.
            // so just delete the existing index files on the table and create new ones
            if (condition == null) {
                // TODO delete exisitng index files for the table
                //and create new ones;
            } else {
                for (int i = 0; i < metaData.columnNameAttrs.size(); i++) {
                    if (metaData.columnNameAttrs.get(i).hasIndex) {
                        RandomAccessFile indexFile = new RandomAccessFile(DavisBase.getNDXFilePath(tableName, metaData.columnNameAttrs.get(i).columnName), "rw");
                        BTree bTree = new BTree(indexFile);
                        for (TableRecord record : deletedRecords) {
                            bTree.delete(record.getAttributes().get(i), record.rowId);
                        }
                    }
                }
            }

            System.out.println();
            tableFile.close();
            System.out.println("Record(s) deleted " + " from " + tableName);

        } catch (Exception e) {
            System.out.println("Error on deleting rows in table : " + tableName);
            System.out.println(e.getMessage());
        }

    }

    public static void parseUserCommand(String userCommand) {

        /*
         * commandTokens is an array of Strings that contains one token per array
         * element The first token can be used to determine the type of command The
         * other tokens can be used to pass relevant parameters to each command-specific
         * method inside each case statement
         */
        ArrayList<String> commandTokens = new ArrayList<>(Arrays.asList(userCommand.split(" ")));

        /*
         * This switch handles a very small list of hardcoded commands of known syntax.
         * You will want to rewrite this method to interpret more complex commands.
         */
        switch (commandTokens.get(0)) {
            case "show":
                if (commandTokens.get(1).equals("tables"))
                    parseUserCommand("select * from davisbase_tables");
                else if (commandTokens.get(1).equals("rowid")) {
                    DavisBaseBinaryFile.showRowId = true;
                    System.out.println("Table Select will now include RowId.");
                } else
                    System.out.println("Please recheck the command: \"" + userCommand + "\"");
                break;
            case "select":
                parseQuery(userCommand);
                break;
            case "drop":
                dropTable(userCommand);
                break;
            case "create":
                if (commandTokens.get(1).equals("table"))
                    parseCreateTable(userCommand);
                else if (commandTokens.get(1).equals("index"))
                    parseCreateIndex(userCommand);
                break;
            case "update":
                parseUpdate(userCommand);
                break;
            case "insert":
                parseInsert(userCommand);
                break;
            case "delete":
                parseDelete(userCommand);
                break;
            case "help":
                DavisBase.help();
                break;
            case "version":
                DavisBase.displayVersion();
                break;
            case "exit":
            case "quit":
                DavisBase.isExit = true;
                break;
//            case "test":
//                Command.test();
//                break;
            default:
                System.out.println("Please recheck the command: \"" + userCommand + "\"");
                break;
        }
    }

    public enum OperatorType {
        LESSTHAN,
        EQUALTO,
        GREATERTHAN,
        LESSTHANOREQUAL,
        GREATERTHANOREQUAL,
        NOTEQUAL,
        INVALID
    }

    /* Command.Condition class

        This class handles logic for where clause*/
    public static class Condition {
        String columnName;
        private OperatorType operator;
        String comparisonValue;
        boolean negation;
        public int columnOrdinal;
        public DataType dataType;

        public Condition(DataType dataType) {
            this.dataType = dataType;
        }

        public static String[] supportedOperators = {"<=", ">=", "<>", ">", "<", "="};

        // Converts the operator string from the user input to Command.OperatorType
        public static OperatorType getOperatorType(String strOperator) {
            switch (strOperator) {
                case ">":
                    return OperatorType.GREATERTHAN;
                case "<":
                    return OperatorType.LESSTHAN;
                case "=":
                    return OperatorType.EQUALTO;
                case ">=":
                    return OperatorType.GREATERTHANOREQUAL;
                case "<=":
                    return OperatorType.LESSTHANOREQUAL;
                case "<>":
                    return OperatorType.NOTEQUAL;
                default:
                    out.println("! Invalid operator \"" + strOperator + "\"");
                    return OperatorType.INVALID;
            }
        }

        public static int compare(String value1, String value2, DataType dataType) {
            if (dataType == DataType.TEXT)
                return value1.toLowerCase().compareTo(value2);
            else if (dataType == DataType.NULL) {
                if (Objects.equals(value1, value2))
                    return 0;
                else if (value1.equalsIgnoreCase("null"))
                    return 1;
                else
                    return -1;
            } else {
                return Long.valueOf(Long.parseLong(value1) - Long.parseLong(value2)).intValue();
            }
        }

        private boolean doOperationOnDifference(OperatorType operation, int difference) {
            switch (operation) {
                case LESSTHANOREQUAL:
                    return difference <= 0;
                case GREATERTHANOREQUAL:
                    return difference >= 0;
                case NOTEQUAL:
                    return difference != 0;
                case LESSTHAN:
                    return difference < 0;
                case GREATERTHAN:
                    return difference > 0;
                case EQUALTO:
                    return difference == 0;
                default:
                    return false;
            }
        }

        private boolean doStringCompare(String currentValue, OperatorType operation) {
            return doOperationOnDifference(operation, currentValue.toLowerCase().compareTo(comparisonValue));
        }

        // Does comparison on currentvalue with the comparison value
        public boolean checkCondition(String currentValue) {
            OperatorType operation = getOperation();

            if (currentValue.equalsIgnoreCase("null")
                    || comparisonValue.equalsIgnoreCase("null"))
                return doOperationOnDifference(operation, compare(currentValue, comparisonValue, DataType.NULL));

            if (dataType == DataType.TEXT || dataType == DataType.NULL)
                return doStringCompare(currentValue, operation);
            else {

                switch (operation) {
                    case LESSTHANOREQUAL:
                        return Long.parseLong(currentValue) <= Long.parseLong(comparisonValue);
                    case GREATERTHANOREQUAL:
                        return Long.parseLong(currentValue) >= Long.parseLong(comparisonValue);

                    case NOTEQUAL:
                        return Long.parseLong(currentValue) != Long.parseLong(comparisonValue);
                    case LESSTHAN:
                        return Long.parseLong(currentValue) < Long.parseLong(comparisonValue);

                    case GREATERTHAN:
                        return Long.parseLong(currentValue) > Long.parseLong(comparisonValue);
                    case EQUALTO:
                        return Long.parseLong(currentValue) == Long.parseLong(comparisonValue);

                    default:
                        return false;

                }

            }

        }

        public void setConditionValue(String conditionValue) {
            this.comparisonValue = conditionValue;
            this.comparisonValue = comparisonValue.replace("'", "");
            this.comparisonValue = comparisonValue.replace("\"", "");

        }

        public void setColumName(String columnName) {
            this.columnName = columnName;
        }

        public void setOperator(String operator) {
            this.operator = getOperatorType(operator);
        }

        public void setNegation(boolean negate) {
            this.negation = negate;
        }

        public OperatorType getOperation() {
            if (!negation)
                return this.operator;
            else
                return negateOperator();
        }

        // In case of NOT operator, invert the operator
        private OperatorType negateOperator() {
            switch (this.operator) {
                case LESSTHANOREQUAL:
                    return OperatorType.GREATERTHAN;
                case GREATERTHANOREQUAL:
                    return OperatorType.LESSTHAN;
                case NOTEQUAL:
                    return OperatorType.EQUALTO;
                case LESSTHAN:
                    return OperatorType.GREATERTHANOREQUAL;
                case GREATERTHAN:
                    return OperatorType.LESSTHANOREQUAL;
                case EQUALTO:
                    return OperatorType.NOTEQUAL;
                default:
                    out.println("! Invalid operator \"" + this.operator + "\"");
                    return OperatorType.INVALID;
            }
        }
    }
}
