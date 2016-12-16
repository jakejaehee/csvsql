import jake.csvsql.Conditions;
import jake.csvsql.ListSql;
import jake.csvsql.ListSqlUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jakejaehee on 2016-12-16.
 */
public class CSVSqlTest {
    public static void main(String[] args) {
        List<Map> csvList = new ArrayList<Map>();
        Map record = new HashMap();
        Conditions conditions = ListSqlUtil.extractConditions(record,
                new String[] { "PLANT" });

        List<Map> rows = ListSql.select(csvList, conditions);
    }
}
