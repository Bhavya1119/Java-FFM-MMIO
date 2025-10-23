package io.ffm;

import io.ffm.reader.impl.CSVReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.FieldVector;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Author - Bhavya Joshi
 */
public class Driver {

    public static Path getFilePath(String fileName) {
        try {
            URL resource = ClassLoader.getSystemClassLoader().getResource(fileName);
            if (resource == null) {
                throw new IllegalArgumentException("File not found in resources: " + fileName);
            }
            return Paths.get(resource.toURI());
        } catch (Exception e) {
            throw new RuntimeException("Failed to resolve file path for: " + fileName, e);
        }
    }

     static void main(String[] args) {
        Path filePath = getFilePath("games.csv");

        try {
            System.out.println("Reading File : " + filePath);
            var reader = new CSVReader();

            long start = System.currentTimeMillis();
            VectorSchemaRoot root = reader.readToArrow(filePath);
            long elapsed = System.currentTimeMillis() - start;

            System.out.printf("Parsed %d rows from %s in %d ms%n",
                    root.getRowCount(), filePath.getFileName(), elapsed);

            printSampleRows(root, 100);

            root.close();
        } catch (Exception ex) {
            System.err.println("Exception while reading file to off heap : " + ex);
        }
    }

    private static void printSampleRows(VectorSchemaRoot root, int limit) {
        var fields = root.getSchema().getFields();
        var vectors = root.getFieldVectors();
        int rowCount = Math.min(limit, root.getRowCount());

        System.out.println("\n--- Sample Rows ---");
        for (int i = 0; i < rowCount; i++) {
            StringBuilder sb = new StringBuilder("{ ");
            for (int j = 0; j < fields.size(); j++) {
                FieldVector vector = vectors.get(j);
                Object value = vector.getObject(i);
                sb.append(fields.get(j).getName())
                        .append(": ")
                        .append(value)
                        .append(j < fields.size() - 1 ? ", " : "");
            }
            sb.append(" }");
            System.out.println(sb);
        }
        System.out.println("-------------------\n");
    }
}
