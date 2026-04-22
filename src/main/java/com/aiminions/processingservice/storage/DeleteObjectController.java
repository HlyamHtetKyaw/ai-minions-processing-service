package com.aiminions.processingservice.storage;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/internal/storage")
@RequiredArgsConstructor
public class DeleteObjectController {

	private final ObjectStorageTransferService objectStorageTransferService;

	@PostMapping("/delete")
	public Map<String, Object> delete(@RequestBody Map<String, Object> body) {
		Object raw = body == null ? null : body.get("key");
		String key = raw instanceof String s ? s.trim() : "";
		if (key.isBlank()) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "key is required");
		}
		boolean deleted = objectStorageTransferService.deleteObject(key);
		return Map.of("deleted", deleted);
	}
}

