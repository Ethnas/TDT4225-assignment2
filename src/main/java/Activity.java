import lombok.Data;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Data
public class Activity {
    public int id;
    public int userId;
    public String transportMode = null;
    public Timestamp startDateTime;
    public Timestamp endDateTime;
    public List<TrackPoint> trackPoints = new ArrayList<>();
}
