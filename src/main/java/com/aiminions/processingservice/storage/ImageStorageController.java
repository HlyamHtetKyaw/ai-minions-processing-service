package com.aiminions.processingservice.storage;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.aiminions.processingservice.storage.dto.ImageStoreRequest;
import com.aiminions.processingservice.storage.dto.ImageStoreResponse;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/v1/internal/storage")
@RequiredArgsConstructor
public class ImageStorageController {

	private final ObjectStorageTransferService objectStorageTransferService;

	@PostMapping("/images")
	public ImageStoreResponse storeImage(@RequestBody ImageStoreRequest request) {
		ObjectStorageTransferService.StoredObject stored = objectStorageTransferService
				.uploadPng(request.imageBytes(), request.keyHint());
		return new ImageStoreResponse(stored.storageUrl(), stored.key());
	}
}
