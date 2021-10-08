public class Main {

    public static void main(String[] args) {
//        deleteAndCreateTables();
//        MyCsvParser csv = new MyCsvParser();
//        csv.readActivityFiles();
        DbConnector dbConnector = new DbConnector();
        dbConnector.getConnection();
        dbConnector.solveTask1();
        dbConnector.closeConnection();

    }
    private static void deleteAndCreateTables() {
        DbConnector db = new DbConnector();
        db.getConnection();
        db.deleteAllTables();
        db.createTables();
    }

}
