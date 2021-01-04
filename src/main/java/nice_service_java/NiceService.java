package nice_service_java;

import org.apache.spark.api.java.JavaRDD;

public class NiceService {
    public long countSomething(JavaRDD<String> lines) {
        return lines.filter(s -> s.length() > 3).count();

    }
}