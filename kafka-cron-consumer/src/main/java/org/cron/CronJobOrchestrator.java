package org.cron;

import org.example.config.KafkaConfiguration;
import org.example.consumer.DefaultKafkaConsumerFactory;
import org.example.consumer.IKafkaConsumerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CronJobOrchestrator {

    private static final Logger LOGGER = LoggerFactory.getLogger(CronJobOrchestrator.class);
    private static final long IDLE_THRESHOLD_MS = 60 * 1000; // 1 minute of silence = Done
    private static final long MAX_JOB_DURATION_MS = 45 * 60 * 100;

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        LOGGER.info("Starting Daily Order Dump Cron Job...");
        WikiMediaConsumer wikiMediaConsumer = null;

        try {

            KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
            IKafkaConsumerFactory<String , String> consumerFactory = new DefaultKafkaConsumerFactory<>(kafkaConfiguration);
            wikiMediaConsumer = new WikiMediaConsumer(consumerFactory.createConsumer(Util.loadApplicationProperties()),"wiki-media");
            Thread t = new Thread(wikiMediaConsumer);
            t.start();

            boolean jobFinished = false;
            while (!jobFinished) {

                Thread.sleep(10000);
                long currentTime = System.currentTimeMillis();
                long lastRecordTime = wikiMediaConsumer.getLastRecordTimestamp();
                long timeSinceLastRecord = currentTime - lastRecordTime;
                long totalRunTime = currentTime - startTime;

                LOGGER.info("Job Status: Running for {}s | Idle for {}s",
                        totalRunTime/1000, timeSinceLastRecord/1000);

                if (totalRunTime > 20000 && timeSinceLastRecord > IDLE_THRESHOLD_MS) {
                    LOGGER.info("✅ IDLE DETECTED: No new records for {}ms. Assuming batch complete.", IDLE_THRESHOLD_MS);
                    jobFinished = true;
                }

                if (totalRunTime > MAX_JOB_DURATION_MS) {
                    LOGGER.error("⚠️ HARD TIMEOUT: Job exceeded max duration of {}ms. Forcing shutdown.", MAX_JOB_DURATION_MS);
                    jobFinished = true;
                }
            }

        } catch (Exception e) {
            LOGGER.error("FATAL: Job crashed unexpectedly.", e);
            System.exit(1);
        } finally {
            LOGGER.info("Shutting down consumer...");
            if (wikiMediaConsumer != null) {
                wikiMediaConsumer.shutDownConsumer();
            }
            LOGGER.info("Cron Job Run Completed.");
        }
    }

}