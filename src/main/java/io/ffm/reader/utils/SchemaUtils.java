package io.ffm.reader.utils;

import io.ffm.reader.schema.ArrowSchema;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Author - Bhavya Joshi
 */
public final class SchemaUtils {

    private static final byte QUOTE = '"';
    private static final byte LF = '\n';
    private static final int FIELD_BUFFER_SIZE = 256;

    private static final DateTimeFormatter[] DATE_FORMATTERS = {
            DateTimeFormatter.ISO_LOCAL_DATE,
            DateTimeFormatter.ofPattern("yyyy/MM/dd"),
            DateTimeFormatter.ofPattern("MM/dd/yyyy"),
            DateTimeFormatter.ofPattern("dd-MM-yyyy")
    };

    private SchemaUtils() {}

    public static ArrowSchema inferSchema(MemorySegment segment, long size,
                                          byte delimiter, int sampleSize, boolean hasHeader) {
        long[] position = {0};

        List<String> headers = extractHeaders(segment, size, delimiter, position, hasHeader);
        List<MinorType> types = inferTypes(segment, size, delimiter, position, headers.size(), sampleSize);
        Schema schema = buildSchema(headers, types);

        return new ArrowSchema(schema, headers, types, position[0]);
    }

    private static List<String> extractHeaders(MemorySegment segment, long size, byte delimiter, long[] position, boolean hasHeader) {
        List<String> headers = new ArrayList<>(16);
        byte[] buffer = new byte[FIELD_BUFFER_SIZE];
        int bufferPos = 0;
        boolean inQuotes = false;

        while (position[0] < size) {
            byte currentByte = segment.get(ValueLayout.JAVA_BYTE, position[0]++);

            if (currentByte == QUOTE) {
                inQuotes = !inQuotes;
            } else if (!inQuotes) {
                if (currentByte == delimiter) {
                    headers.add(createString(buffer, bufferPos));
                    bufferPos = 0;
                } else if (currentByte == LF) {
                    headers.add(createString(buffer, bufferPos));
                    break;
                } else {
                    if (bufferPos < FIELD_BUFFER_SIZE) {
                        buffer[bufferPos++] = currentByte;
                    }
                }
            } else {
                if (bufferPos < FIELD_BUFFER_SIZE) {
                    buffer[bufferPos++] = currentByte;
                }
            }
        }

        if (!hasHeader) {
            for (int i = 0; i < headers.size(); i++) {
                headers.set(i, "col_" + i);
            }
            position[0] = 0;
        }

        return headers;
    }

    /**
     * STATISTICAL DATA TYPE INFERENCE ENGINE
     * 
     * This method implements a sophisticated statistical approach to infer column data types
     * from CSV data stored in off-heap memory. The inference works in three phases:
     * 
     * PHASE 1: SAMPLING & PARSING
     * - Reads a configurable sample of rows (default: 10) from memory-mapped CSV
     * - Parses each field byte-by-byte, handling CSV quoting rules
     * - Maintains position tracking through the memory segment
     * 
     * PHASE 2: HIERARCHICAL TYPE DETECTION
     * - For each field, attempts type detection in priority order:
     *   1. BOOLEAN: Checks for true/false, yes/no, 1/0, t/f, y/n patterns
     *   2. INTEGER: Validates numeric sequence with optional +/- signs
     *   3. LONG: Same as integer (distinction made during final determination)
     *   4. DOUBLE: Checks for decimal points, scientific notation (1.23e-4)
     *   5. DATE: Attempts parsing with multiple date formats (ISO, US, European)
     *   6. VARCHAR: Default fallback for any unmatched patterns
     * 
     * PHASE 3: STATISTICAL CONSENSUS
     * - Uses 80% consistency threshold for type determination
     * - If 80%+ of non-empty values match a type, column gets that type
     * - Handles mixed data gracefully by falling back to VARCHAR
     * - Empty fields are tracked separately and don't affect type decisions
     */
    private static List<MinorType> inferTypes(MemorySegment segment, long size, byte delimiter, long[] position, int columnCount, int sampleSize) {
        int[][] typeCounts = new int[columnCount][6]; // [bool, int, long, double, date, empty]
        byte[] buffer = new byte[FIELD_BUFFER_SIZE];
        int rowsProcessed = 0;

        while (position[0] < size && rowsProcessed < sampleSize) {
            int bufferPos = 0, col = 0;
            boolean inQuotes = false;

            while (position[0] < size && col < columnCount) {
                byte b = segment.get(ValueLayout.JAVA_BYTE, position[0]++);

                if (b == QUOTE) {
                    inQuotes = !inQuotes;
                } else if (!inQuotes) {
                    if (b == delimiter) {
                        analyzeFieldType(buffer, bufferPos, typeCounts[col++]);
                        bufferPos = 0;
                    } else if (b == LF) {
                        analyzeFieldType(buffer, bufferPos, typeCounts[col]);
                        rowsProcessed++;
                        break;
                    } else if (bufferPos < FIELD_BUFFER_SIZE) {
                        buffer[bufferPos++] = b;
                    }
                } else if (bufferPos < FIELD_BUFFER_SIZE) {
                    buffer[bufferPos++] = b;
                }
            }
        }

        return determineTypes(typeCounts, rowsProcessed, columnCount);
    }

    /**
     * Analyzes a single field and increments the appropriate type counter.
     * Uses priority-based detection to ensure most specific types are caught first.
     */
    private static void analyzeFieldType(byte[] buffer, int length, int[] counts) {
        if (length == 0) {
            counts[5]++; //for empty fields
            return;
        }

        switch (detectType(buffer, length)) {
            case 0 -> counts[0]++; // boolean
            case 1 -> counts[1]++; // int
            case 2 -> counts[2]++; // long  
            case 3 -> counts[3]++; // double
            case 4 -> counts[4]++; // date
        }
    }

    /**
     * Returns integer codes for detected types in priority order.
     * Higher priority types are checked first to ensure specificity.
     */
    private static int detectType(byte[] buffer, int length) {
        if (tryBoolean(buffer, length)) return 0;
        if (tryInt(buffer, length)) return 1;
        if (tryLong(buffer, length)) return 2;
        if (tryDouble(buffer, length)) return 3;
        if (tryDate(buffer, length)) return 4;
        return 5;
    }

    private static List<MinorType> determineTypes(int[][] typeCounts, int totalRows, int columnCount) {
        List<MinorType> types = new ArrayList<>(columnCount);
        final double THRESHOLD = 0.8;

        for (int col = 0; col < columnCount; col++) {
            int nonEmpty = totalRows - typeCounts[col][5];
            if (nonEmpty == 0) {
                types.add(MinorType.VARCHAR);
                continue;
            }

            int threshold = (int) (nonEmpty * THRESHOLD);
            int[] counts = typeCounts[col];

            if (counts[0] >= threshold) types.add(MinorType.BIT);
            else if (counts[1] >= threshold) types.add(MinorType.INT);
            else if (counts[2] >= threshold) types.add(MinorType.BIGINT);
            else if (counts[3] >= threshold) types.add(MinorType.FLOAT8);
            else if (counts[4] >= threshold) types.add(MinorType.DATEDAY);
            else types.add(MinorType.VARCHAR);
        }

        return types;
    }

    private static boolean tryBoolean(byte[] buffer, int length) {
        if (length == 1) {
            byte b = buffer[0];
            return b == '0' || b == '1' || b == 'y' || b == 'Y' ||
                    b == 'n' || b == 'N' || b == 't' || b == 'T' || b == 'f' || b == 'F';
        }

        String value = createString(buffer, length).toLowerCase();
        return value.equals("true") || value.equals("false") ||
                value.equals("yes") || value.equals("no") ||
                value.equals("y") || value.equals("n");
    }

    private static boolean tryInt(byte[] buffer, int length) {
        return isIntegerSequence(buffer, length);
    }

    private static boolean tryLong(byte[] buffer, int length) {
        return isIntegerSequence(buffer, length);
    }

    private static boolean tryDouble(byte[] buffer, int length) {
        int i = 0;
        while (i < length && buffer[i] == ' ') i++;

        if (i < length && (buffer[i] == '-' || buffer[i] == '+')) i++;

        boolean hasDigit = false;
        boolean hasDot = false;

        while (i < length) {
            byte b = buffer[i];
            if (b >= '0' && b <= '9') {
                hasDigit = true;
                i++;
            } else if (b == '.' && !hasDot) {
                hasDot = true;
                i++;
            } else if ((b == 'e' || b == 'E') && hasDigit) {
                i++;
                if (i < length && (buffer[i] == '-' || buffer[i] == '+')) i++;
                break;
            } else if (b == ' ') {
                break;
            } else {
                return false;
            }
        }

        // Consume remaining digits after exponent
        while (i < length && buffer[i] >= '0' && buffer[i] <= '9') i++;
        while (i < length && buffer[i] == ' ') i++;

        return hasDigit && i == length;
    }

    private static boolean tryDate(byte[] buffer, int length) {
        String dateStr = createString(buffer, length);
        for (DateTimeFormatter formatter : DATE_FORMATTERS) {
            try {
                LocalDate.parse(dateStr, formatter);
                return true;
            } catch (Exception ignored) {
            }
        }
        return false;
    }

    private static boolean isIntegerSequence(byte[] buffer, int length) {
        int i = 0;
        while (i < length && buffer[i] == ' ') i++;

        if (i < length && (buffer[i] == '-' || buffer[i] == '+')) i++;
        if (i >= length) return false;

        boolean hasDigit = false;
        while (i < length && buffer[i] >= '0' && buffer[i] <= '9') {
            hasDigit = true;
            i++;
        }

        while (i < length && buffer[i] == ' ') i++;
        return hasDigit && i == length;
    }

    private static String createString(byte[] buffer, int length) {
        return new String(buffer, 0, length, StandardCharsets.UTF_8).trim();
    }

    private static Schema buildSchema(List<String> headers, List<MinorType> types) {
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < headers.size(); i++) {
            fields.add(new Field(
                    headers.get(i),
                    FieldType.nullable(types.get(i).getType()),
                    null
            ));
        }
        return new Schema(fields);
    }

    public static int estimateRowCount(MemorySegment segment, long size) {
        final long SAMPLE_SIZE = 65536L;
        long sampleEnd = Math.min(size, SAMPLE_SIZE);
        int newlineCount = 0;

        for (long i = 0; i < sampleEnd; i++) {
            if (segment.get(ValueLayout.JAVA_BYTE, i) == LF) {
                newlineCount++;
            }
        }

        if (newlineCount == 0) return 1000;

        long estimatedTotal = (size * newlineCount) / sampleEnd;
        return (int) Math.min(estimatedTotal * 11 / 10, Integer.MAX_VALUE / 2);
    }


}