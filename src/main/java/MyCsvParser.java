import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

public class MyCsvParser {
    DbConnector db = new DbConnector();
    MyFileReader myFileReader = new MyFileReader();
    Path startPath = Paths.get("dataset/dataset/Data");

    public MyCsvParser() {
        myFileReader.getLabelIds();
    }

    public void readDataLineByLine(List<Path> paths, Path parentPath) {
        db.getConnection();
        User user = new User();
        user.setId(parentPath.getFileName().toString());
        if (myFileReader.labeledIds.contains(String.valueOf(user.id))) {
            user.setHasLabel(true);
        }
        db.createUser(user);
        for (Path path : paths) {
            List<TrackPoint> trackPoints = new ArrayList<>();
            try {
                boolean addActivity = true;
                String labelMatch = null;
                Activity activity = new Activity();
                activity.setUserId(Integer.parseInt(user.id));
                long count = Files.lines(path).count();
                if (count <= 2506) {
                    BufferedReader br = new BufferedReader(new FileReader(path.toString()));
                    String line;
                    for (int i = 1; i <= 6; i++) {
                        br.readLine();
                    }
                    while ((line = br.readLine()) != null) {
                        String[] data = line.split(",");
                        TrackPoint trackPoint = new TrackPoint();
                        trackPoint.setLat(Double.parseDouble(data[0]));
                        trackPoint.setLon(Double.parseDouble(data[1]));
                        trackPoint.setAlt((int) Double.parseDouble(data[3]));
                        trackPoint.setDateDays(Double.parseDouble(data[4]));
                        trackPoint.setDateString(data[5]);
                        trackPoint.setTime(data[6]);
                        trackPoint.setDateTimeCombined();
                        trackPoints.add(trackPoint);
                    }
                    br.close();
                    setActivityStartAndEndTime(trackPoints, activity);
                    if (user.isHasLabel()) {
                        labelMatch = checkStartEndTimesMatchLabel(activity, parentPath);
                        if (labelMatch != null) {
                            String[] parts = labelMatch.split("\t");
                            activity.setTransportMode(parts[2]);
                        } else {
                            addActivity = false;
                        }
                        System.out.println("UserID: " + user.id + " matches" + Objects.requireNonNullElse(labelMatch, "No label matches found"));
                    }
                    if (addActivity) {
                        setActivityIdForTrackPoints(trackPoints, activity);
                        db.createTrackPoints(trackPoints);
                    }
                } else {
                    System.out.println("Has more than 2500 lines");
                }
            } catch (IOException | ParseException | SQLException e) {
                e.printStackTrace();
            }
        }
        System.out.println("User " + user.id + " is completed");
        db.closeConnection();
    }

    private void setActivityStartAndEndTime(List<TrackPoint> trackPoints, Activity activity) throws ParseException {
        TrackPoint firstTrackPoint = trackPoints.get(0);
        TrackPoint lastTrackPoint = trackPoints.get(trackPoints.size() - 1);
        String startDate = firstTrackPoint.getDateString() + " " + firstTrackPoint.getTime();
        String endDate = lastTrackPoint.getDateString() + " " + lastTrackPoint.getTime();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date parsedStartDate = dateFormat.parse(startDate);
        Date parsedEndDate = dateFormat.parse(endDate);
        Timestamp timestamp = new Timestamp(parsedStartDate.getTime());
        activity.setStartDateTime(timestamp);
        timestamp = new Timestamp(parsedEndDate.getTime());
        activity.setEndDateTime(timestamp);
    }

    private String checkStartEndTimesMatchLabel(Activity activity, Path path) throws ParseException {
        List<String> labels = myFileReader.getLabelsTxt(path);
        List<String> matches = labels.stream()
                .filter(s -> s.contains(activity.getStartDateTime().toString().replace(".0", "").replace("-", "/")))
                .filter(s -> s.contains(activity.getEndDateTime().toString().replace(".0", "").replace("-", "/")))
                .toList();
        if (matches.isEmpty()) {
            return null;
        } else {
            return matches.get(0);
        }
    }

    public void readActivityFiles() {
        try {
            List<Path> paths = myFileReader.findUserDirectories(startPath);
            for (Path p : paths) {
                List<Path> activityFiles = myFileReader.findByFileExtension(p, "plt");
                readDataLineByLine(activityFiles, p);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void setActivityIdForTrackPoints(List<TrackPoint> trackPoints, Activity activity) throws SQLException {
        int createdId = db.createActivity(activity);
        if (createdId != -1) {
            for (TrackPoint tp : trackPoints) {
                tp.setActivityId(createdId);
            }
        } else {
            throw new SQLException();
        }

    }
}
