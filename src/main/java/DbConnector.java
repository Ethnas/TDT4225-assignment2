import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DbConnector {
    Connection connection;
    String url = "jdbc:mysql://192.168.27.128:3306/assignment2";
    String username = "erlend";
    String password = "";
    static final String QUERYING = "Querying: ";
    private static final String CREATE_USER_TABLE = "CREATE TABLE IF NOT EXISTS User ("
            + "id INT NOT NULL PRIMARY KEY,"
            + "has_labels BOOLEAN NOT NULL )";
    private static final String CREATE_ACTIVITY_TABLE = "CREATE TABLE IF NOT EXISTS Activity ("
            + "id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,"
            + "user_id INT,"
            + "transportation_mode VARCHAR(255),"
            + "start_date_time DATETIME,"
            + "end_date_time DATETIME,"
            + "FOREIGN KEY (user_id) REFERENCES User(id))";
    private static final String CREATE_TRACKPOINT_TABLE = "CREATE TABLE IF NOT EXISTS Trackpoint ("
            + "id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,"
            + "activity_id INT,"
            + "lat DOUBLE NOT NULL,"
            + "lon DOUBLE NOT NULL,"
            + "altitude INT NOT NULL,"
            + "date_days DOUBLE NOT NULL,"
            + "date_time DATETIME NOT NULL,"
            + "FOREIGN KEY (activity_id) REFERENCES Activity(id))";


    public void getConnection() {
        try {
            connection = DriverManager.getConnection(url, username, password);
            System.out.println("Driver loaded");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            connection = null;
        }
    }

    public void closeConnection() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createTables() {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            stmt.executeUpdate(CREATE_USER_TABLE);
            stmt.executeUpdate(CREATE_ACTIVITY_TABLE);
            stmt.executeUpdate(CREATE_TRACKPOINT_TABLE);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }


    public void deleteAllTables() {
        deleteTrackpointTable();
        deleteActivityTable();
        deleteUserTable();
    }

    public void deleteUserTable() {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            String sql = "DROP TABLE IF EXISTS User";
            stmt.executeUpdate(sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    public void deleteActivityTable() {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
             String sql = "DROP TABLE IF EXISTS Activity";
            stmt.executeUpdate(sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    public void deleteTrackpointTable() {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            String sql = "DROP TABLE IF EXISTS TrackPoint ";
            stmt.executeUpdate(sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    //Inserts a user in the User table
    public void createUser(User user) {
        PreparedStatement stmt = null;
        try {
            String sql = "INSERT INTO User (id, has_labels) VALUES (?,?)";
            stmt = connection.prepareStatement(sql);
            stmt.setInt(1, Integer.parseInt(user.id));
            stmt.setBoolean(2, user.hasLabel);
            stmt.executeUpdate();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    //Inserts an activity in the Activity table
    public int createActivity(Activity activity) {
        ResultSet rs = null;
        int returnValue = -1;
        PreparedStatement stmt = null;
        try {
            String sql = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES (?, ?, ?, ?)";
            stmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            stmt.setInt(1, activity.userId);
            stmt.setString(2, activity.transportMode);
            stmt.setTimestamp(3, activity.startDateTime);
            stmt.setTimestamp(4, activity.endDateTime);
            stmt.executeUpdate();
            rs = stmt.getGeneratedKeys();
            if (rs.first()) {
                returnValue = rs.getInt(1);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
        return returnValue;
    }

    // Does a batch insert of all the trackpoints in the given list to the Trackpoint table
    public void createTrackPoints(List<TrackPoint> trackPoints) {
        PreparedStatement stmt = null;
        try {
            connection.setAutoCommit(false);
            String sql = "INSERT INTO Trackpoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (?, ?, ?, ?, ?, ?)";
            stmt = connection.prepareStatement(sql);
            for (TrackPoint tp : trackPoints) {
                stmt.setInt(1, tp.getActivityId());
                stmt.setDouble(2, tp.getLat());
                stmt.setDouble(3, tp.getLon());
                stmt.setInt(4, tp.getAlt());
                stmt.setDouble(5, tp.getDateDays());
                stmt.setTimestamp(6, tp.getDateTime());
                stmt.addBatch();
                stmt.clearParameters();
            }
            stmt.executeBatch();
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    //----------------------- Below follows the queries used to solve the tasks in Part 2 -----------------------//

    // How many users, activities and trackpoints are there in the dataset (after it is
    // inserted into the database).
    public void solveTask1() {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            String sql = "SELECT COUNT(id) FROM User";
            ResultSet rs = stmt.executeQuery(sql);
            System.out.println(QUERYING + sql);
            DBTablePrinter.printResultSet(rs);
            sql = "SELECT COUNT(id) FROM Activity;";
            rs = stmt.executeQuery(sql);
            System.out.println(QUERYING + sql);
            DBTablePrinter.printResultSet(rs);
            sql = "SELECT COUNT(id) FROM Trackpoint;";
            rs = stmt.executeQuery(sql);
            System.out.println(QUERYING + sql);
            DBTablePrinter.printResultSet(rs);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if ((stmt != null)) {
                    stmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    // Find the average, minimum and maximum number of activities per user.
    public void solveTask2() {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            String sql = "SELECT AVG(activities) FROM (SELECT COUNT(*) activities FROM User INNER JOIN Activity A on User.id = A.user_id GROUP BY User.id) as Activities";
            ResultSet rs = stmt.executeQuery(sql);
            System.out.println(QUERYING + sql);
            DBTablePrinter.printResultSet(rs);
            sql = "SELECT MAX(activities) FROM (SELECT COUNT(*) activities FROM User INNER JOIN Activity A on User.id = A.user_id GROUP BY User.id) as Activities";
            rs = stmt.executeQuery(sql);
            System.out.println(QUERYING + sql);
            DBTablePrinter.printResultSet(rs);
            sql = "SELECT MIN(activities) FROM (SELECT COUNT(*) activities FROM User INNER JOIN Activity A on User.id = A.user_id GROUP BY User.id) as Activities";
            rs = stmt.executeQuery(sql);
            System.out.println(QUERYING + sql);
            DBTablePrinter.printResultSet(rs);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if ((stmt != null)) {
                    stmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    // Find the top 10 users with the highest number of activities.
    public void solveTask3() {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            String sql = "SELECT U.id, COUNT(*) FROM User U INNER JOIN Activity A on U.id = A.user_id GROUP BY U.id ORDER BY COUNT(*) DESC LIMIT 10";
            ResultSet rs = stmt.executeQuery(sql);
            System.out.println(QUERYING + sql);
            DBTablePrinter.printResultSet(rs);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if ((stmt != null)) {
                    stmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    // Find the number of users that have started the activity in one day and ended
    // the activity the next day.
    public void solveTask4() {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            String sql = "SELECT COUNT(*) FROM User U INNER JOIN Activity A on U.id = A.user_id WHERE DATEDIFF(A.end_date_time, A.start_date_time) = 1";
            ResultSet rs = stmt.executeQuery(sql);
            System.out.println(QUERYING + sql);
            DBTablePrinter.printResultSet(rs);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if ((stmt != null)) {
                    stmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    // Find activities that are registered multiple times. You should find the query
    // even if you get zero results.
    public void solveTask5() {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            String sql = "SELECT * FROM Activity A1 INNER JOIN Activity A2 ON A1.id != A2.id AND A1.user_id = A2.user_id AND A1.start_date_time = A2.start_date_time AND A1.end_date_time = A2.end_date_time";
            ResultSet rs = stmt.executeQuery(sql);
            System.out.println(QUERYING + sql);
            DBTablePrinter.printResultSet(rs);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if ((stmt != null)) {
                    stmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    // Find the number of users which have been close to each other in time and
    // space (Covid-19 tracking). Close is defined as the same minute (60 seconds)
    // and space (100 meters).
    public void solveTask6() {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            String sql = "SELECT COUNT(A.user_id) FROM Activity A INNER JOIN\n" +
                    "    (SELECT DISTINCT T1.activity_id as T1aId, T2.activity_id as T2aId FROM Trackpoint T1 CROSS JOIN Trackpoint T2\n" +
                    "    WHERE (ST_Distance_Sphere(POINT(T1.lon, T1.lat), POINT(T2.lon, T2.lat)) <= 100)\n" +
                    "      AND (ABS(TIMESTAMPDIFF(SECOND, T1.date_time, T2.date_time)) <= 60) AND T1.activity_id <> T2.activity_id) AS T ON A.id = T.T1aId";
            ResultSet rs = stmt.executeQuery(sql);
            System.out.println(QUERYING + sql);
            DBTablePrinter.printResultSet(rs);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if ((stmt != null)) {
                    stmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    //  Find all users that have never taken a taxi.
    public void solveTask7() {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            String sql = "SELECT DISTINCT U.id FROM User U INNER JOIN Activity A on U.id = A.user_id WHERE A.transportation_mode != 'taxi'";
            ResultSet rs = stmt.executeQuery(sql);
            System.out.println(QUERYING + sql);
            DBTablePrinter.printResultSet(rs);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if ((stmt != null)) {
                    stmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    // Find all types of transportation modes and count how many distinct users that
    // have used the different transportation modes. Do not count the rows where the
    // transportation mode is null.
    public void solveTask8() {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            String sql = "SELECT DISTINCT transportation_mode FROM Activity WHERE transportation_mode IS NOT NULL";
            ResultSet rs = stmt.executeQuery(sql);
            System.out.println(QUERYING + sql);
            DBTablePrinter.printResultSet(rs);
            sql = "SELECT DISTINCT A.transportation_mode, COUNT(DISTINCT U.id) FROM User U INNER JOIN Activity A on U.id = A.user_id WHERE A.transportation_mode IS NOT NULL GROUP BY transportation_mode";
            rs = stmt.executeQuery(sql);
            System.out.println(QUERYING + sql);
            DBTablePrinter.printResultSet(rs);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if ((stmt != null)) {
                    stmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    // Find the year and month with the most activities
    public void solveTask9a() {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            String sql = "SELECT EXTRACT(YEAR_MONTH FROM start_date_time) date_YYYYMM, COUNT(*) number_of FROM Activity GROUP BY EXTRACT(YEAR_MONTH FROM start_date_time) ORDER BY number_of DESC LIMIT 1";
            ResultSet rs = stmt.executeQuery(sql);
            System.out.println(QUERYING + sql);
            DBTablePrinter.printResultSet(rs);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if ((stmt != null)) {
                    stmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    public void solveTask9b() {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            String sql = "";
            stmt.executeUpdate(sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if ((stmt != null)) {
                    stmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    // Find the total distance (in km) walked in 2008, by user with id=112.
    public void solveTask10() {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            String sql = "SELECT * FROM Trackpoint T INNER JOIN (SELECT * FROM Activity A WHERE A.user_id = 112 AND EXTRACT(YEAR FROM A.start_date_time) = 2008 AND A.transportation_mode = 'walk') A ON T.activity_id = A.id\n" +
                    "WHERE (T.date_time = A.start_date_time OR T.date_time = A.end_date_time)";
            ResultSet rs = stmt.executeQuery(sql);
            List<TrackPoint> trackPoints = new ArrayList<>();
            while (rs.next()) {
                TrackPoint trackPoint = new TrackPoint();
                trackPoint.setActivityId(rs.getInt(2));
                trackPoint.setLat(rs.getDouble(3));
                trackPoint.setLon(rs.getDouble(4));
                trackPoints.add(trackPoint);
            }
            double distance = 0;
            if (!trackPoints.isEmpty()) {
                for (int i = 0; i < trackPoints.size() - 2; i++) {
                    if (trackPoints.get(i).activityId == trackPoints.get(i + 1).activityId) {
                        distance += calculateDistanceInKilometer(trackPoints.get(i).lat, trackPoints.get(i).lon,
                                trackPoints.get(i + 1).lat, trackPoints.get(i + 1).lon);
                    }
                }
            }
            System.out.println("Total distance walked by user 112 in 2008 is " + distance);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if ((stmt != null)) {
                    stmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    public void solveTask11() {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            String sql = "";
            stmt.executeUpdate(sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if ((stmt != null)) {
                    stmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    public void solveTask12() {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            String sql = "";
            stmt.executeUpdate(sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if ((stmt != null)) {
                    stmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    private void printResult(ResultSet rs, String query) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        System.out.println("Querying " + query);
        int columnsNumber = rsmd.getColumnCount();
        while (rs.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) System.out.println(",  ");
                String columnValue = rs.getString(i);
                System.out.println(columnValue + " " + rsmd.getColumnName(i));
            }
            System.out.println("");
        }
    }

    //Copied from
    //https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula/12600225#12600225
    public final static double AVERAGE_RADIUS_OF_EARTH_KM = 6371;
    public double calculateDistanceInKilometer(double userLat, double userLng,
                                            double venueLat, double venueLng) {

        double latDistance = Math.toRadians(userLat - venueLat);
        double lngDistance = Math.toRadians(userLng - venueLng);

        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(userLat)) * Math.cos(Math.toRadians(venueLat))
                * Math.sin(lngDistance / 2) * Math.sin(lngDistance / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return AVERAGE_RADIUS_OF_EARTH_KM * c;

    }

}
