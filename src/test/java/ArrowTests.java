/**
 * @author zongkx
 */

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Arrays;


/**
 * @author zongkx
 */

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ArrowTests {

    @BeforeAll
    public void before() {


    }


    @Test
    @SneakyThrows
    void read1() {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM generate_series(2000)");
             DuckDBResultSet resultSet = (DuckDBResultSet) stmt.executeQuery();
             RootAllocator allocator = new RootAllocator()) {
            try (ArrowReader reader = (ArrowReader) resultSet.arrowExportStream(allocator, 256)) {
                while (reader.loadNextBatch()) {
                    System.out.println(reader.getVectorSchemaRoot().getVector("generate_series"));
                }
            }
        }
    }


    @Test
    @SneakyThrows
    void read2() {
        RootAllocator allocator = new RootAllocator();
        ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(get(allocator)), allocator); // should not be null of course
        ArrowArrayStream arrow_array_stream = ArrowArrayStream.allocateNew(allocator);
        Data.exportArrayStream(allocator, reader, arrow_array_stream);
        DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();
        conn.registerArrowStream("asdf", arrow_array_stream);
        DuckDBResultSet rs = (DuckDBResultSet) stmt.executeQuery("SELECT * FROM asdf");
        while (rs.next()) {
            System.out.println(rs.getInt(1));
            System.out.println(rs.getString(2));
        }
    }


    @SneakyThrows
    private byte[] get(RootAllocator rootAllocator) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(User.schema(), rootAllocator);
        int total = 10;
        vectorSchemaRoot.setRowCount(total);
        for (int i = 0; i < total; i++) {
            vectorizeUser(i, new User(i, "测试"), vectorSchemaRoot);
        }
        ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, Channels.newChannel(out));
        writer.start();
        writer.writeBatch();
        return out.toByteArray();// new ArrowStreamReader(new ByteArrayInputStream(), rootAllocator);
    }


    private void vectorizeUser(int index, User user, VectorSchemaRoot schemaRoot) {
        ((VarCharVector) schemaRoot.getVector("name")).setSafe(index, user.getName().getBytes());
        ((BigIntVector) schemaRoot.getVector("id")).setSafe(index, user.getId());
    }

    @lombok.Data
    @AllArgsConstructor
    static class User {
        int id;
        String name;

        public static Schema schema() {
            Field id = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
            Field wave = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
            return new Schema(Arrays.asList(id, wave));
        }
    }

}