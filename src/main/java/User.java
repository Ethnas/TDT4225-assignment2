import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class User {
    public String id;
    public boolean hasLabel = false;
    public List<Activity> activities = new ArrayList<>();
}
