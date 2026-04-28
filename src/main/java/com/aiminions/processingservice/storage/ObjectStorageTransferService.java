package com.aiminions.processingservice.storage;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Service;

import com.aiminions.processingservice.config.WorkerStorageProperties;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.HttpMethod;
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
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.core.sync.RequestBody;

@Service
@RequiredArgsConstructor
@Slf4j
public class ObjectStorageTransferService {

	private static final HttpClient HTTP = HttpClient.newBuilder()
			.connectTimeout(Duration.ofSeconds(30))
			.followRedirects(HttpClient.Redirect.NORMAL)
			.build();
	private static final String DEFAULT_IMAGE_PREFIX = "content-generator/v2";

	private final WorkerStorageProperties props;

	private volatile S3Client s3Client;
	private volatile S3Presigner s3Presigner;
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

	private void ensureS3Presigner() {
		if (s3Presigner != null) {
			return;
		}
		synchronized (this) {
			if (s3Presigner != null) {
				return;
			}
			var builder = S3Presigner.builder().region(Region.of(props.getRegion().trim()));
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
			this.s3Presigner = builder.build();
			log.info("S3 presigner ready");
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

	/**
	 * Strips a URI fragment (e.g. {@code #wk=...} from the main-service media refresh) so
	 * HTTP/HTTPS fetches and {@link HttpRequest} do not see a bogus fragment, and the query
	 * string for GCS presigned URLs stays intact.
	 */
	public static String stripUrlFragmentForDownload(String storageUrl) {
		if (storageUrl == null) {
			return null;
		}
		String t = storageUrl.trim();
		int h = t.indexOf('#');
		return h < 0 ? t : t.substring(0, h);
	}

	public Path download(String storageUrl, Path workDir) throws IOException, InterruptedException {
		WorkspaceObjectRef workspaceObjectRef = parseWorkspaceObjectRef(storageUrl);
		if (workspaceObjectRef != null) {
			try {
				return downloadWorkspaceObjectRef(workspaceObjectRef, workDir);
			} catch (Exception ex) {
				log.warn("Workspace-key direct download failed, falling back to URL: {}", workspaceObjectRef.key(), ex);
			}
		}
		String forDownload = stripUrlFragmentForDownload(storageUrl);
		URI u = URI.create(forDownload.trim());
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

	private Path downloadWorkspaceObjectRef(WorkspaceObjectRef ref, Path workDir) throws IOException {
		String bucket = ref.bucket() == null || ref.bucket().isBlank() ? props.getBucket() : ref.bucket();
		if (bucket == null || bucket.isBlank()) {
			throw new IOException("Storage bucket is not configured for workspace object download");
		}
		String key = ref.key();
		String name = Path.of(key).getFileName().toString();
		if (name.isBlank()) {
			name = "download.bin";
		}
		Path out = workDir.resolve(sanitizeLocalName(name));

		// Use provider hint from URL when available so mixed configs still resolve workspace media.
		if (ref.providerHint() == StorageProviderHint.GCP) {
			ensureGcsClient();
			byte[] data = gcs.readAllBytes(BlobId.of(bucket, key));
			Files.write(out, data);
			return out;
		}
		if (ref.providerHint() == StorageProviderHint.S3) {
			ensureS3Client();
			GetObjectRequest get = GetObjectRequest.builder().bucket(bucket).key(key).build();
			s3Client.getObject(get, out);
			return out;
		}
		if (props.getProvider() == WorkerStorageProperties.Provider.GCP) {
			ensureGcsClient();
			byte[] data = gcs.readAllBytes(BlobId.of(bucket, key));
			Files.write(out, data);
			return out;
		}
		ensureS3Client();
		GetObjectRequest get = GetObjectRequest.builder().bucket(bucket).key(key).build();
		s3Client.getObject(get, out);
		return out;
	}

	private WorkspaceObjectRef parseWorkspaceObjectRef(String storageUrl) {
		if (storageUrl == null || storageUrl.isBlank()) {
			return null;
		}
		String trimmed = storageUrl.trim();
		int hashIdx = trimmed.indexOf('#');
		if (hashIdx < 0 || hashIdx >= trimmed.length() - 1) {
			return null;
		}
		String fragment = trimmed.substring(hashIdx + 1);
		String workspaceKey = null;
		for (String token : fragment.split("&")) {
			int eq = token.indexOf('=');
			String k = eq >= 0 ? token.substring(0, eq) : token;
			String v = eq >= 0 ? token.substring(eq + 1) : "";
			if ("wk".equals(k) && !v.isBlank()) {
				workspaceKey = URLDecoder.decode(v, StandardCharsets.UTF_8);
				break;
			}
		}
		if (workspaceKey == null || workspaceKey.isBlank()) {
			return null;
		}
		String bucket = null;
		StorageProviderHint providerHint = StorageProviderHint.UNKNOWN;
		try {
			String noFragment = stripUrlFragmentForDownload(trimmed);
			URI u = URI.create(noFragment);
			String host = u.getHost();
			String path = u.getPath() == null ? "" : u.getPath();
			// https://storage.googleapis.com/{bucket}/{key}
			if (host != null && host.equalsIgnoreCase("storage.googleapis.com")) {
				providerHint = StorageProviderHint.GCP;
				String clean = stripLeadingSlash(path);
				int slash = clean.indexOf('/');
				if (slash > 0) {
					bucket = clean.substring(0, slash);
				}
			// https://{bucket}.storage.googleapis.com/{key}
			} else if (host != null && host.endsWith(".storage.googleapis.com")) {
				providerHint = StorageProviderHint.GCP;
				bucket = host.substring(0, host.length() - ".storage.googleapis.com".length());
			// s3://{bucket}/{key}, gs://{bucket}/{key}
			} else if (host != null && ("s3".equalsIgnoreCase(u.getScheme()) || "gs".equalsIgnoreCase(u.getScheme()))) {
				providerHint = "gs".equalsIgnoreCase(u.getScheme()) ? StorageProviderHint.GCP : StorageProviderHint.S3;
				bucket = host;
			} else if (host != null && host.contains("amazonaws.com")) {
				providerHint = StorageProviderHint.S3;
			}
		} catch (Exception ignored) {
			// Best-effort; fallback bucket from properties.
		}
		return new WorkspaceObjectRef(bucket, workspaceKey, providerHint);
	}

	private static final String HTTP_DOWNLOAD_USER_AGENT =
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
					+ "Chrome/120.0.0.0 Safari/537.36 ai-minions-export/1.0";

	/**
	 * Many CDNs (e.g. Pinterest {@code i.pinimg.com}) require a page {@code Referer} or they return 403.
	 */
	private static String httpRefererFor(URI u) {
		if (u == null) {
			return null;
		}
		String host = u.getHost();
		if (host == null || host.isBlank()) {
			return null;
		}
		String h = host.toLowerCase();
		if (h.endsWith("pinimg.com") || h.contains("pinterest.com")) {
			return "https://www.pinterest.com/";
		}
		String scheme = u.getScheme() != null && !u.getScheme().isBlank() ? u.getScheme() : "https";
		return scheme + "://" + host + "/";
	}

	private Path httpDownload(URI u, Path workDir) throws IOException, InterruptedException {
		String name = Path.of(u.getPath()).getFileName().toString();
		if (name == null || name.isBlank() || name.equals("/")) {
			name = "download.bin";
		}
		Path out = workDir.resolve(sanitizeLocalName(name));
		var b = HttpRequest.newBuilder(u)
				.timeout(Duration.ofMinutes(30))
				.header("User-Agent", HTTP_DOWNLOAD_USER_AGENT)
				.header("Accept", "*/*")
				.header("Accept-Language", "en-US,en;q=0.9");
		String ref = httpRefererFor(u);
		if (ref != null) {
			b = b.header("Referer", ref);
		}
		HttpRequest req = b.GET().build();
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

	private enum StorageProviderHint { UNKNOWN, GCP, S3 }

	private record WorkspaceObjectRef(String bucket, String key, StorageProviderHint providerHint) {}

	public StoredObject uploadPng(byte[] bytes, String keyHint) {
		if (bytes == null || bytes.length == 0) {
			throw new IllegalArgumentException("image bytes must not be empty");
		}
		String key = buildKey(keyHint);
		if (props.getProvider() == WorkerStorageProperties.Provider.GCP) {
			ensureGcsClient();
			gcs.create(
					com.google.cloud.storage.BlobInfo.newBuilder(
							BlobId.of(props.getBucket(), key))
							.setContentType("image/png")
							.build(),
					bytes);
			return new StoredObject(buildStorageUrl(props.getBucket(), key), key);
		}
		ensureS3Client();
		PutObjectRequest put = PutObjectRequest.builder()
				.bucket(props.getBucket())
				.key(key)
				.contentType("image/png")
				.build();
		s3Client.putObject(put, RequestBody.fromBytes(bytes));
		return new StoredObject(buildStorageUrl(props.getBucket(), key), key);
	}

	public StoredObject uploadAudio(byte[] bytes, String keyHint, String contentType) {
		if (bytes == null || bytes.length == 0) {
			throw new IllegalArgumentException("audio bytes must not be empty");
		}
		String ct = contentType == null || contentType.isBlank() ? "audio/mpeg" : contentType.trim();
		String key = buildAudioKey(keyHint, ct);
		if (props.getProvider() == WorkerStorageProperties.Provider.GCP) {
			ensureGcsClient();
			gcs.create(
					BlobInfo.newBuilder(props.getBucket(), key).setContentType(ct).build(),
					bytes);
			return new StoredObject(buildStorageUrl(props.getBucket(), key), key);
		}
		ensureS3Client();
		PutObjectRequest put = PutObjectRequest.builder()
				.bucket(props.getBucket())
				.key(key)
				.contentType(ct)
				.build();
		s3Client.putObject(put, RequestBody.fromBytes(bytes));
		return new StoredObject(buildStorageUrl(props.getBucket(), key), key);
	}

	public PresignedObject presignWorkspaceUpload(String keyHint, String contentType) {
		String key = buildWorkspaceAssetKey(keyHint);
		String ct = contentType == null || contentType.isBlank() ? "application/octet-stream" : contentType.trim();
		int ttl = Math.max(60, props.getVideoEditPresignTtlSeconds());
		if (props.getProvider() == WorkerStorageProperties.Provider.GCP) {
			ensureGcsClient();
			BlobInfo blobInfo = BlobInfo.newBuilder(props.getBucket(), key).setContentType(ct).build();
			String uploadUrl = gcs.signUrl(
					blobInfo,
					ttl,
					TimeUnit.SECONDS,
					Storage.SignUrlOption.httpMethod(HttpMethod.PUT),
					Storage.SignUrlOption.withV4Signature(),
					Storage.SignUrlOption.withContentType())
					.toString();
			String readUrl = gcs.signUrl(
					BlobInfo.newBuilder(props.getBucket(), key).build(),
					ttl,
					TimeUnit.SECONDS,
					Storage.SignUrlOption.httpMethod(HttpMethod.GET),
					Storage.SignUrlOption.withV4Signature())
					.toString();
			return new PresignedObject(uploadUrl, readUrl, key);
		}

		ensureS3Presigner();
		PutObjectRequest put = PutObjectRequest.builder()
				.bucket(props.getBucket())
				.key(key)
				.contentType(ct)
				.build();
		PutObjectPresignRequest putPresign = PutObjectPresignRequest.builder()
				.signatureDuration(Duration.ofSeconds(ttl))
				.putObjectRequest(put)
				.build();
		GetObjectRequest get = GetObjectRequest.builder()
				.bucket(props.getBucket())
				.key(key)
				.build();
		GetObjectPresignRequest getPresign = GetObjectPresignRequest.builder()
				.signatureDuration(Duration.ofSeconds(ttl))
				.getObjectRequest(get)
				.build();
		return new PresignedObject(
				s3Presigner.presignPutObject(putPresign).url().toString(),
				s3Presigner.presignGetObject(getPresign).url().toString(),
				key);
	}

	public StoredObject uploadFile(Path source, String keyHint, String contentType) throws IOException {
		if (source == null || !Files.exists(source)) {
			throw new IllegalArgumentException("source file does not exist");
		}
		String key = buildWorkspaceExportKey(keyHint, source.getFileName().toString());
		String ct = contentType == null || contentType.isBlank() ? "application/octet-stream" : contentType.trim();
		if (props.getProvider() == WorkerStorageProperties.Provider.GCP) {
			ensureGcsClient();
			byte[] bytes = Files.readAllBytes(source);
			gcs.create(
					BlobInfo.newBuilder(props.getBucket(), key).setContentType(ct).build(),
					bytes);
			return new StoredObject(buildStorageUrl(props.getBucket(), key), key);
		}
		ensureS3Client();
		PutObjectRequest put = PutObjectRequest.builder()
				.bucket(props.getBucket())
				.key(key)
				.contentType(ct)
				.build();
		s3Client.putObject(put, source);
		return new StoredObject(buildStorageUrl(props.getBucket(), key), key);
	}

	public String presignWorkspaceRead(String key) {
		String normalizedKey = stripLeadingSlash(key == null ? "" : key.trim());
		if (normalizedKey.isBlank()) {
			throw new IllegalArgumentException("key is required");
		}
		normalizedKey = resolveExistingWorkspaceReadKey(normalizedKey);
		int ttl = Math.max(60, props.getVideoEditPresignTtlSeconds());
		if (props.getProvider() == WorkerStorageProperties.Provider.GCP) {
			ensureGcsClient();
			return gcs.signUrl(
					BlobInfo.newBuilder(props.getBucket(), normalizedKey).build(),
					ttl,
					TimeUnit.SECONDS,
					Storage.SignUrlOption.httpMethod(HttpMethod.GET),
					Storage.SignUrlOption.withV4Signature())
					.toString();
		}
		ensureS3Presigner();
		GetObjectRequest get = GetObjectRequest.builder()
				.bucket(props.getBucket())
				.key(normalizedKey)
				.build();
		GetObjectPresignRequest getPresign = GetObjectPresignRequest.builder()
				.signatureDuration(Duration.ofSeconds(ttl))
				.getObjectRequest(get)
				.build();
		return s3Presigner.presignGetObject(getPresign).url().toString();
	}

	public String resolveExistingWorkspaceReadKey(String key) {
		if (existsObject(key)) {
			return key;
		}
		if (hasExtension(key)) {
			return key;
		}
		String[] extCandidates = {".mp4", ".mov", ".webm", ".m4a", ".mp3", ".wav", ".aac"};
		for (String ext : extCandidates) {
			String candidate = key + ext;
			if (existsObject(candidate)) {
				return candidate;
			}
		}
		return key;
	}

	public boolean deleteObject(String key) {
		String normalizedKey = stripLeadingSlash(key == null ? "" : key.trim());
		if (normalizedKey.isBlank()) {
			return false;
		}
		if (props.getProvider() == WorkerStorageProperties.Provider.GCP) {
			ensureGcsClient();
			return gcs.delete(BlobId.of(props.getBucket(), normalizedKey));
		}
		ensureS3Client();
		try {
			s3Client.deleteObject(DeleteObjectRequest.builder()
					.bucket(props.getBucket())
					.key(normalizedKey)
					.build());
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	private String buildKey(String keyHint) {
		if (keyHint != null && !keyHint.isBlank()) {
			String normalized = stripLeadingSlash(keyHint.trim());
			return normalized.endsWith(".png") ? normalized : normalized + ".png";
		}
		return DEFAULT_IMAGE_PREFIX + "/generated-" + System.currentTimeMillis() + ".png";
	}

	private String buildWorkspaceAssetKey(String keyHint) {
		if (keyHint != null && !keyHint.isBlank()) {
			return stripLeadingSlash(keyHint.trim());
		}
		return "video-editor/assets/" + System.currentTimeMillis() + ".bin";
	}

	private String buildAudioKey(String keyHint, String contentType) {
		if (keyHint != null && !keyHint.isBlank()) {
			String normalized = stripLeadingSlash(keyHint.trim());
			if (hasExtension(normalized)) {
				return normalized;
			}
			return normalized + extensionFromContentType(contentType);
		}
		return "voice-over/generated-" + System.currentTimeMillis() + extensionFromContentType(contentType);
	}

	private String extensionFromContentType(String contentType) {
		String ct = contentType == null ? "" : contentType.trim().toLowerCase();
		return switch (ct) {
			case "audio/wav", "audio/x-wav" -> ".wav";
			case "audio/mp4", "audio/m4a", "audio/x-m4a" -> ".m4a";
			case "audio/ogg", "audio/opus" -> ".ogg";
			case "audio/webm" -> ".webm";
			default -> ".mp3";
		};
	}

	private String buildWorkspaceExportKey(String keyHint, String fallbackName) {
		if (keyHint != null && !keyHint.isBlank()) {
			return stripLeadingSlash(keyHint.trim());
		}
		String safeName = sanitizeLocalName(fallbackName == null ? "export.mp4" : fallbackName);
		return "video-editor/exports/" + System.currentTimeMillis() + "-" + safeName;
	}

	private boolean existsObject(String key) {
		if (props.getProvider() == WorkerStorageProperties.Provider.GCP) {
			ensureGcsClient();
			return gcs.get(BlobId.of(props.getBucket(), key)) != null;
		}
		ensureS3Client();
		try {
			s3Client.headObject(HeadObjectRequest.builder()
					.bucket(props.getBucket())
					.key(key)
					.build());
			return true;
		} catch (NoSuchKeyException e) {
			return false;
		} catch (Exception e) {
			return false;
		}
	}

	private boolean hasExtension(String key) {
		int slash = key.lastIndexOf('/');
		int dot = key.lastIndexOf('.');
		return dot > slash;
	}

	private String buildStorageUrl(String bucket, String key) {
		if (props.getProvider() == WorkerStorageProperties.Provider.GCP) {
			return "gs://" + bucket + "/" + key;
		}
		return "s3://" + bucket + "/" + key;
	}

	public record StoredObject(
			String storageUrl,
			String key
	) {
	}

	public record PresignedObject(
			String uploadUrl,
			String readUrl,
			String key
	) {
	}
}
