package org.Kafka;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;
import static spark.Spark.*;

import akka.util.Switch;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.Kafka.module.MentorModel;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import static spark.Spark.get;

public class CSVKafkaProducer {
    private static String KafkaBrokerEndpoint = "localhost:9092";
    private static String KafkaTopic = "demo";
    private static String CsvFile = "1-RSD.csv";


    private Producer<String, String> ProducerProperties(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBrokerEndpoint);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaCsvProducer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String, String>(properties);
    }

    public static void main(String[] args) throws URISyntaxException {
        get("/Stream/Start", (req, res)->{
            CSVKafkaProducer kafkaProducer = new CSVKafkaProducer();
            kafkaProducer.PublishMessages();
            System.out.println("Producing job completed");
            return "Streaming is Completed.";
        });

    }
    private void PublishMessages() throws URISyntaxException{

        final Producer<String, String> csvProducer = ProducerProperties();

        try{

            URI uri = getClass().getClassLoader().getResource(CsvFile).toURI();
            Stream<String> FileStream = Files.lines(Paths.get(uri));
            Rest thread=new Rest(FileStream,csvProducer,KafkaTopic);
            thread.run();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class Rest implements Runnable{
    private Stream<String> FileStream;
    private String KafkaTopic;
    private Producer<String, String> csvProducer;

    public void run() {
        FileStream.forEach(line -> {
            try {
//                System.out.println(line.getClass().getName());
                String[] arrOfString=line.split(",",13);
                System.out.println();
                MentorModel mentor=ConToObj(arrOfString);
                ObjectMapper objectMapper = new ObjectMapper();
                String lineAsString=objectMapper.writeValueAsString(mentor);

                final ProducerRecord<String, String> csvRecord = new ProducerRecord<String, String>(
                        KafkaTopic, UUID.randomUUID().toString(), lineAsString);

                csvProducer.send(csvRecord, (metadata, exception) -> {
                    if(metadata != null){
                        System.out.println("CsvData: -> "+ csvRecord.key()+" | "+ csvRecord.value());
                    }
                    else{
                        System.out.println("Error Sending Csv Record -> "+ csvRecord.value());
                    }
                });
                Thread.sleep(3000);
            } catch (Exception e) {
                System.out.println(e);
            }

        });

    }
    public Rest(Stream<String> fileStream,Producer<String, String> CSVProducer,String KafkaTopic) {
            this.FileStream=fileStream;
            this.KafkaTopic=KafkaTopic;
            this.csvProducer=CSVProducer;
    }
    public MentorModel ConToObj(String[] arrOfString){
        MentorModel curMentor=new MentorModel();
        for(int i=0;i<arrOfString.length;i++){
//                    System.out.println(arrOfString[i]);
            switch (i) {
                case 0:
                    curMentor.setAbsentHours(Double.parseDouble(arrOfString[i].substring(1, arrOfString[i].length() - 1)));
                    break;
                case 1:
                    curMentor.setJobTitle(arrOfString[i].substring(1, arrOfString[i].length() - 1));
                    break;
                case 2:
                    curMentor.setEmployeeNumber(Integer.parseInt(arrOfString[i].substring(1, arrOfString[i].length() - 1)));
                    break;
                case 3:
                    curMentor.setDivision(arrOfString[i].substring(1, arrOfString[i].length() - 1));
                    break;
                case 4:
                    curMentor.setStoreLocation(arrOfString[i].substring(1, arrOfString[i].length() - 1));
                    break;
                case 5:
                    curMentor.setDepartmentName(arrOfString[i].substring(1, arrOfString[i].length() - 1));
                    break;
                case 6:
                    curMentor.setBusinessUnit(arrOfString[i].substring(1, arrOfString[i].length() - 1));
                    break;
                case 7:
                    curMentor.setAge(arrOfString[i].substring(1, arrOfString[i].length() - 1));
                    break;
                case 8:
                    curMentor.setGivenName(arrOfString[i].substring(1, arrOfString[i].length() - 1));
                    break;
                case 9:
                    curMentor.setCity(arrOfString[i].substring(1, arrOfString[i].length() - 1));
                    break;
                case 10:
                    curMentor.setSurname(arrOfString[i].substring(1, arrOfString[i].length() - 1));
                    break;
                case 11:
                    curMentor.setGender(Integer.parseInt(arrOfString[i].substring(1, arrOfString[i].length() - 1)));
                    break;
                case 12:
                    curMentor.setLengthService(Double.parseDouble(arrOfString[i].substring(1, arrOfString[i].length() - 1)));
                    break;
            }
        }
        return curMentor;
    }
}