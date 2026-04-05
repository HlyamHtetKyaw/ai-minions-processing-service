package com.aiminions.processingservice.storage;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

import org.springframework.stereotype.Service;

import com.aiminions.processingservice.config.WorkerStorageProperties;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

@Service
@RequiredArgsConstructor
@Slf4j
public class ObjectStorageTransferService {

	private static final HttpClient HTTP = HttpClient.newBuilder()
			.connectTimeout(Duration.ofSeconds(30))
			.followRedirects(HttpClient.Redirect.NORMAL)
			.build();

	private final WorkerStorageProperties props;

	private volatile S3Client s3Client;
	private volatile Storage gcs;

	@PostConstruct
	void initClients() {
		if (props.getProvider() == WorkerStorageProperties.Provider.GCP) {
			ensureGcsClient();
		} else if (props.getProvider() == WorkerStorageProperties.Provider.AWS
				|| props.getProvider() == WorkerStorageProperties.Provider.DIGITALOCEAN) {
			ensureS3Client();
		}
	}

	private void ensureGcsClient() {
		if (gcs != null) {
			return;
		}
		synchronized (this) {
			if (gcs != null) {
				return;
			}
			try {
				if (props.getGcpCredentialsPath() != null && !props.getGcpCredentialsPath().isBlank()) {
					Path credPath = resolveCredentialsPath(props.getGcpCredentialsPath().trim());
					try (InputStream in = Files.newInputStream(credPath)) {
						GoogleCredentials creds = GoogleCredentials.fromStream(in);
						this.gcs = StorageOptions.newBuilder().setCredentials(creds).build().getService();
					}
				} else {
					this.gcs = StorageOptions.getDefaultInstance().getService();
				}
				log.info("GCS Storage client ready (for gs:// downloads)");
			} catch (IOException e) {
				throw new IllegalStateException(
						"Failed to init GCS client for gs:// downloads. Set app.storage.gcp-credentials-path or "
								+ "GOOGLE_APPLICATION_CREDENTIALS to a service-account JSON, or set APP_STORAGE_PROVIDER=gcp. "
								+ e.getMessage(),
						e);
			}
		}
	}

	private void ensureS3Client() {
		if (s3Client != null) {
			return;
		}
		synchronized (this) {
			if (s3Client != null) {
				return;
			}
			var builder = S3Client.builder().region(Region.of(props.getRegion().trim()));
			if (props.getEndpoint() != null && !props.getEndpoint().isBlank()) {
				builder.endpointOverride(URI.create(props.getEndpoint().trim()));
				builder.serviceConfiguration(
						S3Configuration.builder().pathStyleAccessEnabled(props.isPathStyleAccessEffective()).build());
			}
			if (props.getAwsAccessKeyId() != null && !props.getAwsAccessKeyId().isBlank()
					&& props.getAwsSecretAccessKey() != null && !props.getAwsSecretAccessKey().isBlank()) {
				builder.credentialsProvider(StaticCredentialsProvider.create(
						AwsBasicCredentials.create(props.getAwsAccessKeyId().trim(), props.getAwsSecretAccessKey().trim())));
			} else {
				builder.credentialsProvider(DefaultCredentialsProvider.create());
			}
			this.s3Client = builder.build();
			log.info("S3 client ready (for s3:// downloads)");
		}
	}

	private static Path resolveCredentialsPath(String raw) {
		if (raw.regionMatches(true, 0, "file:", 0, 5)) {
			return Paths.get(URI.create(raw));
		}
		Path p = Paths.get(raw);
		if (!p.isAbsolute()) {
			p = Paths.get(System.getProperty("user.dir", ".")).resolve(p).normalize();
		}
		return p;
	}

	public Path download(String storageUrl, Path workDir) throws IOException, InterruptedException {
		URI u = URI.create(storageUrl.trim());
		String scheme = u.getScheme();
		if (scheme == null) {
			throw new IllegalArgumentException("Unsupported storage URL (no scheme): " + storageUrl);
		}
		return switch (scheme.toLowerCase()) {
			case "http", "https" -> httpDownload(u, workDir);
			case "gs" -> {
				ensureGcsClient();
				yield gcsDownload(u, workDir);
			}
			case "s3" -> {
				ensureS3Client();
				yield s3Download(u, workDir);
			}
			default -> throw new IllegalArgumentException("Unsupported storage URL scheme: " + scheme);
		};
	}

	private Path httpDownload(URI u, Path workDir) throws IOException, InterruptedException {
		String name = Path.of(u.getPath()).getFileName().toString();
		if (name == null || name.isBlank() || name.equals("/")) {
			name = "download.bin";
		}
		Path out = workDir.resolve(sanitizeLocalName(name));
		HttpRequest req = HttpRequest.newBuilder(u).timeout(Duration.ofMinutes(30)).GET().build();
		HttpResponse<byte[]> res = HTTP.send(req, HttpResponse.BodyHandlers.ofByteArray());
		if (res.statusCode() / 100 != 2) {
			throw new IOException("HTTP download failed: " + res.statusCode() + " for " + u);
		}
		Files.write(out, res.body());
		return out;
	}

	private Path gcsDownload(URI u, Path workDir) throws IOException {
		String bucket = u.getHost();
		String key = stripLeadingSlash(u.getPath());
		String name = Path.of(key).getFileName().toString();
		if (name.isBlank()) {
			name = "download.bin";
		}
		Path out = workDir.resolve(sanitizeLocalName(name));
		byte[] data = gcs.readAllBytes(BlobId.of(bucket, key));
		Files.write(out, data);
		return out;
	}

	private Path s3Download(URI u, Path workDir) throws IOException {
		String bucket = u.getHost();
		String key = stripLeadingSlash(u.getPath());
		String name = Path.of(key).getFileName().toString();
		if (name.isBlank()) {
			name = "download.bin";
		}
		Path out = workDir.resolve(sanitizeLocalName(name));
		GetObjectRequest get = GetObjectRequest.builder().bucket(bucket).key(key).build();
		s3Client.getObject(get, out);
		return out;
	}

	private static String stripLeadingSlash(String path) {
		if (path == null || path.isEmpty()) {
			return "";
		}
		return path.startsWith("/") ? path.substring(1) : path;
	}

	private static String sanitizeLocalName(String name) {
		String n = name.replace("\\", "_").replace("/", "_");
		return n.isBlank() ? "download.bin" : n;
	}
}
