
import com.opencsv.bean.CsvBindByPosition;
import lombok.Data;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@Data
public class TrackPoint {
    public int activityId;
    public int id;

    @CsvBindByPosition(position = 0)
    public double lat;

    @CsvBindByPosition(position = 1)
    public double lon;

    @CsvBindByPosition(position = 2)
    public int unused;

    @CsvBindByPosition(position = 3)
    public int alt;

    @CsvBindByPosition(position = 4)
    public double dateDays;

    @CsvBindByPosition(position = 5)
    public String dateString;

    @CsvBindByPosition(position = 6)
    public String time;

    public Timestamp dateTime;

    public void setDateTimeCombined() throws ParseException {
        String dt = dateString + " " + time;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date parsedDate = dateFormat.parse(dt);
        dateTime = new Timestamp(parsedDate.getTime());
    }

}
