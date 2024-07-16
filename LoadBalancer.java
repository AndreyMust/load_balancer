package balancer;

import java.io.IOException;
import java.net.http.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.databind.ObjectMapper;
import static java.lang.Thread.sleep;
import static java.net.HttpURLConnection.HTTP_OK;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

interface JsonSerializable {
    String toJson() throws IOException;
}

class MyDocument implements JsonSerializable {
    private String field1;
    private String field2;

    public String getField1() {
        return field1;
    }
    public void setField1(String field1) {
        this.field1 = field1;
    }
    public String getField2() {
        return field2;
    }
    public void setField2(String field2) {
        this.field2 = field2;
    }

    @Override
    public String toJson() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(this);
    }
}

class CrptDocument implements JsonSerializable {
    private String DescriptionParticipantInn;
    private String doc_id;
    private String doc_status;
    private String doc_type;
    private boolean importRequest;
    private String owner_inn;
    private String participant_inn;
    private String producer_inn;
    private String production_date;
    private String production_type;
    private String reg_date;
    private String reg_number;

    public void setDescriptionParticipantInn(String descriptionParticipantInn) {
        DescriptionParticipantInn = descriptionParticipantInn;
    }
    public void setDoc_id(String doc_id) {
        this.doc_id = doc_id;
    }
    public void setDoc_status(String doc_status) {
        this.doc_status = doc_status;
    }
    public void setDoc_type(String doc_type) {
        this.doc_type = doc_type;
    }
    public void setImportRequest(boolean importRequest) {
        this.importRequest = importRequest;
    }
    public void setOwner_inn(String owner_inn) {
        this.owner_inn = owner_inn;
    }
    public void setParticipant_inn(String participant_inn) {
        this.participant_inn = participant_inn;
    }
    public void setProducer_inn(String producer_inn) {
        this.producer_inn = producer_inn;
    }
    public void setProduction_date(String production_date) {
        this.production_date = production_date;
    }
    public void setProduction_type(String production_type) {
        this.production_type = production_type;
    }
    public void setReg_date(String reg_date) {
        this.reg_date = reg_date;
    }
    public void setReg_number(String reg_number) {
        this.reg_number = reg_number;
    }
    public String getDescriptionParticipantInn() {
        return DescriptionParticipantInn;
    }
    public String getDoc_id() {
        return doc_id;
    }
    public String getDoc_status() {
        return doc_status;
    }
    public String getDoc_type() {
        return doc_type;
    }
    public boolean isImportRequest() {
        return importRequest;
    }
    public String getOwner_inn() {
        return owner_inn;
    }
    public String getParticipant_inn() {
        return participant_inn;
    }
    public String getProducer_inn() {
        return producer_inn;
    }
    public String getProduction_date() {
        return production_date;
    }
    public String getProduction_type() {
        return production_type;
    }
    public String getReg_date() {
        return reg_date;
    }
    public String getReg_number() {
        return reg_number;
    }

    @Override
    public String toJson() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(this);
    }
}

class ShowTimer {
    public static void printTime() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        String formattedNow = now.format(formatter);
        System.out.println("Текущее время: " + formattedNow);
    }

}

interface DocumentSender {
    void sendDocument(String jsonDocument, String signature) throws IOException, InterruptedException;
}

class ConsolSender implements DocumentSender{
    @Override
    public void sendDocument(String jsonDocument, String signature) throws IOException {
        System.out.println(signature);
        ShowTimer.printTime();
        System.out.println(jsonDocument);
        try {
            sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("End");
    }
}

class HTTPSender implements DocumentSender {

    private static final String API_URL = "https://ismp.crpt.ru/api/v3/lk/documents/create";
    //private static final String API_URL = "http://192.168.1.2:3000";


    @Override
    public void sendDocument(String jsonDocument, String signature) throws IOException, InterruptedException {

        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        // Authentication
        String auth = "username" + ":" + "password";
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
        String authHeader = "Basic " + encodedAuth;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(API_URL))
                .timeout(Duration.ofMinutes(1))
                .header("Content-Type", "application/json")
                .header("Signature", signature)
                .header("Authorization", authHeader)
                .POST(HttpRequest.BodyPublishers.ofString(jsonDocument, StandardCharsets.UTF_8))
                .build();
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            int statusCode = response.statusCode();
            ShowTimer.printTime();
            System.out.println("Response Code: " + statusCode);
            System.out.println("Response Body: " + response.body());

            if (statusCode != HTTP_OK) {
                throw new IOException("Failed to create document, HTTP response code: " + statusCode);
            }

        } catch (HttpConnectTimeoutException e) {
            System.err.println("Connection timed out: " + e.getMessage());
        } catch (HttpTimeoutException e) {
            System.err.println("Request timed out: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("IO Exception: " + e.getMessage());
            throw e; // Rethrow exception to be caught in createDocument
        } catch (InterruptedException e) {
            System.err.println("Request interrupted: " + e.getMessage());
            Thread.currentThread().interrupt();
            throw e; // Rethrow exception to be caught in createDocument
        }
    }
}

public class LoadBalancer {
    private final Semaphore semaphore;
    private final int requestLimit;
    private final long timeWindowMillis;
    private final DocumentSender documentSender;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService executorService;

    public LoadBalancer(int requestLimit, long timeWindowMillis, DocumentSender documentSender) {
        this.requestLimit = requestLimit;
        this.timeWindowMillis = timeWindowMillis/requestLimit + 10;
        this.semaphore = new Semaphore(1);
        this.documentSender = documentSender;
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.executorService = Executors.newCachedThreadPool();

        // Schedule a task to release permits periodically
        scheduler.scheduleAtFixedRate(() -> {
            semaphore.release(1);
            System.out.println("scheduleAtFixedRate - release");
            }, timeWindowMillis, timeWindowMillis, TimeUnit.MILLISECONDS);
    }

    public void createDocument(JsonSerializable document, String signature) {
        executorService.submit(() -> {
            try {
                semaphore.acquire();
                String jsonDocument = document.toJson();
                documentSender.sendDocument(jsonDocument, signature);
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
                System.out.println(e.toString());
            }
        });
    }

    public void shutdown() {
        scheduler.shutdown();
        System.out.println("scheduler.shutdown()");
        try {
            if (!scheduler.awaitTermination(1, TimeUnit.MINUTES)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        executorService.shutdown();
        System.out.println("executorService.shutdown()");
        try {
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) {
        DocumentSender documentSender = new HTTPSender();
        //DocumentSender documentSender = new ConsolSender();
        LoadBalancer loadBalancer = new LoadBalancer(1, 2000, documentSender);

        for (int i = 0; i < 10; i++) {
            CrptDocument crpt = new CrptDocument();
            crpt.setDoc_id("id_" + i);
            crpt.setDoc_status("Doc_Status555");
            loadBalancer.createDocument(crpt, "signature-string-" + i);
        }
    }
}
