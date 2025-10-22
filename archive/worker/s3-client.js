const { S3Client, PutObjectCommand, GetObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const crypto = require('crypto');
const zlib = require('zlib');
const { promisify } = require('util');

const gzip = promisify(zlib.gzip);
const gunzip = promisify(zlib.gunzip);

class ArtifactStorageClient {
  constructor() {
    // Use DigitalOcean Spaces configuration (compatible with S3 API)
    this.bucket = process.env.OPENSCRAPERS_S3_OBJECT_BUCKET;
    this.region = process.env.DIGITALOCEAN_S3_CLOUD_REGION || 'nyc3';
    this.endpoint = process.env.DIGITALOCEAN_S3_ENDPOINT;
    this.accessKey = process.env.DIGITALOCEAN_S3_ACCESS_KEY;
    this.secretKey = process.env.DIGITALOCEAN_S3_SECRET_KEY;

    if (!this.bucket || !this.endpoint || !this.accessKey || !this.secretKey) {
      console.warn('S3 configuration incomplete - artifact storage will be disabled');
      this.enabled = false;
      return;
    }

    this.s3Client = new S3Client({
      region: this.region,
      endpoint: this.endpoint,
      credentials: {
        accessKeyId: this.accessKey,
        secretAccessKey: this.secretKey,
      },
      forcePathStyle: true, // Required for DigitalOcean Spaces
    });

    this.enabled = true;
    console.log(`S3 artifact storage initialized: bucket=${this.bucket}, endpoint=${this.endpoint}`);
  }

  /**
   * Generate a unique S3 key for a job artifact
   */
  generateArtifactKey(jobId, artifactType = 'result') {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const randomSuffix = crypto.randomBytes(4).toString('hex');
    return `job-artifacts/${jobId}/${timestamp}-${artifactType}-${randomSuffix}`;
  }

  /**
   * Check if result should be stored in S3 based on size
   */
  shouldStoreInS3(data) {
    if (!this.enabled) return false;
    
    const sizeBytes = this.getDataSize(data);
    const maxKafkaSize = 100 * 1024; // 100KB threshold
    
    return sizeBytes > maxKafkaSize;
  }

  /**
   * Get the size of data in bytes
   */
  getDataSize(data) {
    if (typeof data === 'string') {
      return Buffer.byteLength(data, 'utf8');
    }
    if (Buffer.isBuffer(data)) {
      return data.length;
    }
    if (typeof data === 'object') {
      return Buffer.byteLength(JSON.stringify(data), 'utf8');
    }
    return 0;
  }

  /**
   * Compress data if beneficial
   */
  async compressIfBeneficial(data) {
    const originalSize = this.getDataSize(data);
    
    // Only compress text data larger than 1KB
    if (originalSize < 1024 || Buffer.isBuffer(data)) {
      return { data, compressed: false, originalSize, compressedSize: originalSize };
    }

    const dataStr = typeof data === 'string' ? data : JSON.stringify(data);
    const compressed = await gzip(Buffer.from(dataStr, 'utf8'));
    
    // Only use compression if it saves at least 20%
    if (compressed.length < originalSize * 0.8) {
      return { 
        data: compressed, 
        compressed: true, 
        originalSize, 
        compressedSize: compressed.length 
      };
    }

    return { 
      data: dataStr, 
      compressed: false, 
      originalSize, 
      compressedSize: originalSize 
    };
  }

  /**
   * Store artifact in S3 and return storage metadata
   */
  async storeArtifact(jobId, data, artifactType = 'result', metadata = {}) {
    if (!this.enabled) {
      throw new Error('S3 storage not enabled');
    }

    console.log(`Storing artifact for job ${jobId}, type=${artifactType}, size=${this.getDataSize(data)} bytes`);

    // Compress data if beneficial
    const { data: processedData, compressed, originalSize, compressedSize } = await this.compressIfBeneficial(data);

    // Generate unique S3 key
    const s3Key = this.generateArtifactKey(jobId, artifactType);
    const extension = compressed ? '.gz' : (artifactType === 'result' ? '.json' : '.data');
    const fullKey = s3Key + extension;

    // Determine content type
    let contentType = 'application/octet-stream';
    if (compressed) {
      contentType = 'application/gzip';
    } else if (typeof data === 'object' || artifactType === 'result') {
      contentType = 'application/json';
    } else if (typeof data === 'string') {
      contentType = 'text/plain';
    }

    // Upload to S3
    const putCommand = new PutObjectCommand({
      Bucket: this.bucket,
      Key: fullKey,
      Body: processedData,
      ContentType: contentType,
      Metadata: {
        jobId: jobId,
        artifactType: artifactType,
        compressed: compressed.toString(),
        originalSize: originalSize.toString(),
        ...metadata
      }
    });

    await this.s3Client.send(putCommand);

    const storageUrl = `s3://${this.bucket}/${fullKey}`;
    
    console.log(`Successfully stored artifact: ${storageUrl} (${compressedSize} bytes)`);

    return {
      storage_url: storageUrl,
      s3_key: fullKey,
      bucket: this.bucket,
      artifact_metadata: {
        type: artifactType,
        content_type: contentType,
        original_size: originalSize,
        stored_size: compressedSize,
        compressed: compressed,
        stored_at: new Date().toISOString()
      }
    };
  }

  /**
   * Retrieve artifact from S3
   */
  async retrieveArtifact(storageUrl) {
    if (!this.enabled) {
      throw new Error('S3 storage not enabled');
    }

    // Parse S3 URL: s3://bucket/key
    const match = storageUrl.match(/^s3:\/\/([^\/]+)\/(.+)$/);
    if (!match) {
      throw new Error(`Invalid S3 URL format: ${storageUrl}`);
    }

    const [, bucket, key] = match;

    console.log(`Retrieving artifact from S3: bucket=${bucket}, key=${key}`);

    // Get object from S3
    const getCommand = new GetObjectCommand({
      Bucket: bucket,
      Key: key
    });

    const response = await this.s3Client.send(getCommand);
    const bodyBytes = await this.streamToBuffer(response.Body);

    // Check if data is compressed
    const isCompressed = response.Metadata?.compressed === 'true';
    
    if (isCompressed) {
      const decompressed = await gunzip(bodyBytes);
      return decompressed.toString('utf8');
    }

    // Return as string if it's JSON or text
    const contentType = response.ContentType || '';
    if (contentType.includes('json') || contentType.includes('text')) {
      return bodyBytes.toString('utf8');
    }

    // Return raw bytes for binary data
    return bodyBytes;
  }

  /**
   * Check if artifact exists in S3
   */
  async artifactExists(storageUrl) {
    if (!this.enabled) return false;

    const match = storageUrl.match(/^s3:\/\/([^\/]+)\/(.+)$/);
    if (!match) return false;

    const [, bucket, key] = match;

    try {
      const headCommand = new HeadObjectCommand({
        Bucket: bucket,
        Key: key
      });
      
      await this.s3Client.send(headCommand);
      return true;
    } catch (error) {
      if (error.name === 'NotFound') {
        return false;
      }
      throw error;
    }
  }

  /**
   * Convert a readable stream to buffer
   */
  async streamToBuffer(stream) {
    const chunks = [];
    for await (const chunk of stream) {
      chunks.push(chunk);
    }
    return Buffer.concat(chunks);
  }

  /**
   * Create a truncated version of large result for Kafka
   */
  createResultSummary(originalResult, maxSize = 1024) {
    if (typeof originalResult === 'string') {
      return originalResult.length > maxSize 
        ? originalResult.substring(0, maxSize) + '... [truncated]'
        : originalResult;
    }

    if (Array.isArray(originalResult)) {
      return {
        type: 'array',
        length: originalResult.length,
        first_items: originalResult.slice(0, 3),
        truncated: originalResult.length > 3
      };
    }

    if (typeof originalResult === 'object') {
      const summary = { ...originalResult };
      const summaryStr = JSON.stringify(summary);
      
      if (summaryStr.length > maxSize) {
        return {
          type: 'object',
          keys: Object.keys(originalResult),
          preview: JSON.stringify(originalResult).substring(0, maxSize) + '... [truncated]'
        };
      }
      
      return summary;
    }

    return originalResult;
  }
}

module.exports = { ArtifactStorageClient };