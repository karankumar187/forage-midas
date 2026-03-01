package com.jpmc.midascore;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.UserRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UserPopulator {
    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private DatabaseConduit databaseConduit;

    public void populate() {
        String[] userLines = fileLoader.loadStrings("/test_data/lkjhgfdsa.hjkl");
        for (String userLine : userLines) {
            String[] userData = userLine.split(", ");
            UserRecord user = new UserRecord(userData[0], Float.parseFloat(userData[1]));
            databaseConduit.save(user);
        }
    }

    public long findIdByName(String name) {
        // Not perfectly efficient, but good enough for a quick test
        String[] userLines = fileLoader.loadStrings("/test_data/lkjhgfdsa.hjkl");
        for (long i = 1; i <= userLines.length; i++) {
            String[] userData = userLines[(int) (i - 1)].split(", ");
            if (userData[0].equals(name)) {
                return i; // IDs are likely auto-generated starting from 1
            }
        }
        return -1;
    }
}
