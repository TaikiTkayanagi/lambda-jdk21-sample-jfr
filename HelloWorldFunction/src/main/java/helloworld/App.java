package helloworld;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonWriter;

import jdk.jfr.FlightRecorder;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Handler for requests to Lambda function.
 */
public class App implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    private static final Gson GSON = new GsonBuilder().create();

    @Override
    public APIGatewayProxyResponseEvent handleRequest(final APIGatewayProxyRequestEvent input, final Context context) {
        String bucket = "tun-chan-love";
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalStateException("環境変数 JFR_BUCKET にアップロード先バケット名を設定してください。");
        }

        // 出力する JFR ファイル（ローカル）
        Path jfrFile = Paths.get("/tmp/app.jfr");

        // JFR をプログラムから開始
        try {
            var recording = FlightRecorder.getFlightRecorder().getRecordings().stream()
                    .filter(r -> r.getName().equals("app")).findFirst().orElse(null);

            // ★ここが計測したい処理（中くらいの JSON シリアライズ）
            runJsonWorkload();

            // 計測終了
            recording.stop();
            recording.dump(jfrFile);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("JFR written to: " + jfrFile.toAbsolutePath());

        // S3 にアップロード
        try {
            uploadToS3(bucket, jfrFile);
            Files.deleteIfExists(jfrFile);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 「中くらいのサイズ」の JSON を作るための適当なワークロード。
     * ここでは Order のリスト（数千件、各 Order に明細を複数）をシリアライズしています。
     */
    private static void runJsonWorkload() {
        List<Order> orders = generateSampleOrders(3000, 5); // 3000件 x 明細5行 くらい
        long start = System.nanoTime();
        var array = new ByteArrayOutputStream();
        var outputStream = new OutputStreamWriter(array);
        var bufferWriter = new BufferedWriter(outputStream);
        var jsonWriter = new JsonWriter(bufferWriter);
        var type = new TypeToken<List<Order>>() {
        }.getType();
        GSON.toJson(orders, type, jsonWriter);
        long end = System.nanoTime();

        System.out.printf("JSON length = %,d bytes, elapsed = %d ms%n", array.toByteArray().length,
                (end - start) / 1_000_000);
    }

    private static List<Order> generateSampleOrders(int count, int linesPerOrder) {
        List<Order> list = new ArrayList<>(count);
        Random rnd = new Random(0);
        for (int i = 0; i < count; i++) {
            Order o = new Order();
            o.orderId = "ORD-" + (100000 + i);
            o.customerId = "CUST-" + (1000 + (i % 100));
            o.createdAt = Instant.now().minusSeconds(rnd.nextInt(3600 * 24)).toString();
            o.status = (i % 3 == 0) ? "NEW" : (i % 3 == 1) ? "PROCESSING" : "DONE";

            o.lines = new ArrayList<>();
            for (int j = 0; j < linesPerOrder; j++) {
                OrderLine l = new OrderLine();
                l.lineNo = j + 1;
                l.sku = "SKU-" + (1000 + rnd.nextInt(500));
                l.quantity = 1 + rnd.nextInt(10);
                l.unitPrice = 100 + rnd.nextInt(900);
                o.lines.add(l);
            }
            list.add(o);
        }
        return list;
    }

    private static void uploadToS3(String bucket, Path jfrFile) throws Exception {
        if (!Files.exists(jfrFile)) {
            throw new IllegalStateException("JFRファイルが存在しません: " + jfrFile);
        }

        String key = "jfr/gson-" + Instant.now().toString().replace(":", "-") + ".jfr";

        S3Client s3 = S3Client.builder()
                .region(Region.AP_NORTHEAST_1)
                .build();

        System.out.printf("Uploading JFR to s3://%s/%s ...%n", bucket, key);

        s3.putObject(
                PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build(),
                RequestBody.fromFile(jfrFile));

        System.out.println("Upload finished.");
    }

    static class Order {
        String orderId;
        String customerId;
        String createdAt;
        String status;
        List<OrderLine> lines;
    }

    static class OrderLine {
        int lineNo;
        String sku;
        int quantity;
        int unitPrice;
    }
}
