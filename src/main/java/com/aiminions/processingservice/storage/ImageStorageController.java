package com.aiminions.processingservice.storage;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.aiminions.processingservice.storage.dto.ImageStoreRequest;
import com.aiminions.processingservice.storage.dto.ImageStoreResponse;
import com.aiminions.processingservice.storage.dto.AudioStoreRequest;
import com.aiminions.processingservice.storage.dto.AudioStoreResponse;
import com.aiminions.processingservice.storage.dto.StoragePresignReadRequest;
import com.aiminions.processingservice.storage.dto.StoragePresignReadResponse;
import com.aiminions.processingservice.storage.dto.StoragePresignUploadRequest;
import com.aiminions.processingservice.storage.dto.StoragePresignUploadResponse;
import com.aiminions.processingservice.storage.dto.WorkspaceExportRequest;
import com.aiminions.processingservice.storage.dto.WorkspaceExportResponse;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/v1/internal/storage")
@RequiredArgsConstructor
public class ImageStorageController {

	private final ObjectStorageTransferService objectStorageTransferService;
	private final WorkspaceExportService workspaceExportService;

	@PostMapping("/images")
	public ImageStoreResponse storeImage(@RequestBody ImageStoreRequest request) {
		ObjectStorageTransferService.StoredObject stored = objectStorageTransferService
				.uploadPng(request.imageBytes(), request.keyHint());
		return new ImageStoreResponse(stored.storageUrl(), stored.key());
	}

	@PostMapping("/audio")
	public AudioStoreResponse storeAudio(@RequestBody AudioStoreRequest request) {
		ObjectStorageTransferService.StoredObject stored = objectStorageTransferService
				.uploadAudio(request.audioBytes(), request.keyHint(), request.contentType());
		return new AudioStoreResponse(stored.storageUrl(), stored.key());
	}

	@PostMapping("/video-workspace/presign-upload")
	public StoragePresignUploadResponse presignWorkspaceUpload(@RequestBody StoragePresignUploadRequest request) {
		ObjectStorageTransferService.PresignedObject signed = objectStorageTransferService
				.presignWorkspaceUpload(request.keyHint(), request.contentType());
		return new StoragePresignUploadResponse(signed.uploadUrl(), signed.readUrl(), signed.key());
	}

	@PostMapping("/video-workspace/presign-read")
	public StoragePresignReadResponse presignWorkspaceRead(@RequestBody StoragePresignReadRequest request) {
		String resolvedKey = objectStorageTransferService.resolveExistingWorkspaceReadKey(request.key());
		String readUrl = objectStorageTransferService.presignWorkspaceRead(resolvedKey);
		return new StoragePresignReadResponse(readUrl, resolvedKey);
	}

	@PostMapping("/video-workspace/export")
	public WorkspaceExportResponse exportWorkspaceVideo(@RequestBody WorkspaceExportRequest request) {
		return workspaceExportService.exportVideo(request.userId(), request.payload());
	}
}
