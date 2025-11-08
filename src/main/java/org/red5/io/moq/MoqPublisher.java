package org.red5.io.moq;

import org.red5.io.moq.client.MoqClient;
import org.red5.io.moq.model.TrackEvent;
import org.red5.io.moq.processor.CmafStreamProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Red5 Publisher - Processes MP4 stream from ffmpeg and publishes via MOQ Transport.
 */
@Command(
        name = "moqpub",
        description = "Red5 Publisher - Processes MP4 stream and publishes via MOQ Transport",
        mixinStandardHelpOptions = true,
        version = "0.2.0"
)
public class MoqPublisher implements Callable<Integer> {
    private static final Logger logger = LoggerFactory.getLogger(MoqPublisher.class);

    @Option(names = {"--name"}, description = "Stream name to publish", defaultValue = "test")
    private String streamName;

    @Option(names = {"--url"}, description = "MOQ Transport URL", defaultValue = "https://127.0.0.1:8443")
    private String url;

    @Option(names = {"--input-source"}, description = "Input source: stdin or tcp", defaultValue = "stdin")
    private String inputSource;

    @Option(names = {"--tcp-port"}, description = "TCP port for input (when using tcp source)", defaultValue = "12346")
    private int tcpPort;

    private final AtomicBoolean running = new AtomicBoolean(true);

    @Override
    public Integer call() {
        logger.info("Starting MOQTAIL-derived Red5 Publisher");
        logger.info("Stream name: {}", streamName);
        logger.info("URL: {}", url);
        logger.info("Input source: {}", inputSource);

        // Setup shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received");
            running.set(false);
        }));

        // Create event queue for communication between processor and client
        BlockingQueue<List<TrackEvent>> eventQueue = new LinkedBlockingQueue<>();

        // Start MOQ client in separate thread
        MoqClient moqClient = new MoqClient(url, false, eventQueue);
        Thread clientThread = new Thread(() -> {
            try {
                logger.info("Starting MOQ client (certificate verification disabled)");
                moqClient.run();
            } catch (Exception e) {
                logger.error("MOQ client error", e);
            }
        });
        clientThread.setDaemon(false);
        clientThread.start();

        // Give client time to connect
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Note: MP4 processor can start immediately
        // The event loop will buffer frames until PUBLISH_OK responses create subscriptions
        logger.info("Starting MP4 processor (frames will be buffered until subscriptions are ready)...");

        // Start TCP listener if needed (in separate thread)
        if ("tcp".equalsIgnoreCase(inputSource)) {
            Thread listenerThread = new Thread(() -> {
                try {
                    startTcpListener(tcpPort);
                } catch (Exception e) {
                    logger.error("TCP listener error", e);
                }
            });
            listenerThread.setDaemon(true);
            listenerThread.start();
        }

        // Start MP4 processor (using CMAF library)
        Thread processorThread = new Thread(() -> {
            try {
                CmafStreamProcessor processor = new CmafStreamProcessor(eventQueue);

                if ("stdin".equalsIgnoreCase(inputSource)) {
                    logger.info("Processing MP4 stream from stdin...");
                    processor.processStream(System.in);
                } else if ("tcp".equalsIgnoreCase(inputSource)) {
                    logger.info("Connecting to TCP socket 127.0.0.1:{}...", tcpPort);
                    try (Socket socket = new Socket("127.0.0.1", tcpPort)) {
                        logger.info("Connected to TCP socket");
                        processor.processStream(socket.getInputStream());
                    }
                } else {
                    logger.error("Invalid input source: {}", inputSource);
                    return;
                }

                logger.info("Finished processing input stream");
            } catch (IOException e) {
                logger.error("MP4 processor error", e);
            }
        });
        processorThread.setDaemon(false);
        processorThread.start();

        // Wait for threads or shutdown signal
        try {
            while (running.get()) {
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("Main thread interrupted");
        }

        logger.info("Processing complete");
        return 0;
    }

    /**
     * Simple TCP listener that accepts connections and logs data.
     * In a real implementation, this would feed data to the processor.
     */
    private void startTcpListener(int port) {
        logger.info("TCP listener started on 127.0.0.1:{}", port);

        try (java.net.ServerSocket serverSocket = new java.net.ServerSocket(port)) {
            while (running.get()) {
                Socket clientSocket = serverSocket.accept();
                logger.info("TCP client connected: {}", clientSocket.getRemoteSocketAddress());

                Thread handler = new Thread(() -> {
                    try (InputStream in = clientSocket.getInputStream()) {
                        byte[] buffer = new byte[1024];
                        int bytesRead;
                        while ((bytesRead = in.read(buffer)) != -1) {
                            logger.debug("Received {} bytes from TCP client", bytesRead);
                        }
                    } catch (IOException e) {
                        logger.error("Error reading from TCP client", e);
                    } finally {
                        try {
                            clientSocket.close();
                        } catch (IOException e) {
                            logger.error("Error closing TCP client socket", e);
                        }
                    }
                });
                handler.start();
            }
        } catch (IOException e) {
            logger.error("TCP listener error", e);
        }
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new MoqPublisher()).execute(args);
        System.exit(exitCode);
    }

}
