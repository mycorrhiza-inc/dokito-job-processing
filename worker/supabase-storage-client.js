const { createClient } = require('@supabase/supabase-js');
const crypto = require('crypto');
const zlib = require('zlib');
const { promisify } = require('util');

const gzip = promisify(zlib.gzip);
const gunzip = promisify(zlib.gunzip);

class SupabaseArtifactStorageClient {
  constructor() {
    // Use Supabase configuration
    this.supabaseUrl = process.env.SUPABASE_URL;
    this.supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
    this.bucketName = 'job-artifacts'; // Supabase storage bucket

    if (!this.supabaseUrl || !this.supabaseServiceKey) {
      console.warn('Supabase configuration incomplete - artifact storage will be disabled');
      this.enabled = false;
      return;
    }

    this.supabase = createClient(this.supabaseUrl, this.supabaseServiceKey, {
      auth: {
        autoRefreshToken: false,
        persistSession: false
      }
    });

    this.enabled = true;
    console.log(`Supabase artifact storage initialized: url=${this.supabaseUrl}, bucket=${this.bucketName}`);
  }

  /**
   * Initialize storage bucket if it doesn't exist
   */
  async ensureBucketExists() {
    if (!this.enabled) return false;

    try {
      // Try to get bucket info
      const { data, error } = await this.supabase.storage.getBucket(this.bucketName);
      
      if (error && error.statusCode === '404') {
        // Bucket doesn't exist, create it
        console.log(`Creating Supabase storage bucket: ${this.bucketName}`);
        const { data: createData, error: createError } = await this.supabase.storage.createBucket(this.bucketName, {
          public: false, // Private bucket for job artifacts
          allowedMimeTypes: ['application/json', 'application/gzip', 'text/plain', 'application/octet-stream'],
          fileSizeLimit: 50 * 1024 * 1024 // 50MB limit
        });

        if (createError) {
          console.error('Failed to create Supabase storage bucket:', createError);
          return false;
        }
        
        console.log('Successfully created Supabase storage bucket');
      } else if (error) {
        console.error('Error checking Supabase storage bucket:', error);
        return false;
      }

      return true;
    } catch (error) {
      console.error('Error ensuring bucket exists:', error);
      return false;
    }
  }

  /**
   * Generate a unique storage path for a job artifact
   */
  generateArtifactPath(jobId, artifactType = 'result') {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const randomSuffix = crypto.randomBytes(4).toString('hex');
    return `${jobId}/${timestamp}-${artifactType}-${randomSuffix}`;
  }

  /**
   * Check if result should be stored in Supabase based on size
   */
  shouldStoreInSupabase(data) {
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
   * Store artifact in Supabase Storage and return storage metadata
   */
  async storeArtifact(jobId, data, artifactType = 'result', metadata = {}) {
    if (!this.enabled) {
      throw new Error('Supabase storage not enabled');
    }

    // Ensure bucket exists
    const bucketReady = await this.ensureBucketExists();
    if (!bucketReady) {
      throw new Error('Failed to initialize Supabase storage bucket');
    }

    console.log(`Storing artifact for job ${jobId}, type=${artifactType}, size=${this.getDataSize(data)} bytes`);

    // Compress data if beneficial
    const { data: processedData, compressed, originalSize, compressedSize } = await this.compressIfBeneficial(data);

    // Generate unique storage path
    const storagePath = this.generateArtifactPath(jobId, artifactType);
    const extension = compressed ? '.gz' : (artifactType === 'result' ? '.json' : '.data');
    const fullPath = storagePath + extension;

    // Determine content type
    let contentType = 'application/octet-stream';
    if (compressed) {
      contentType = 'application/gzip';
    } else if (typeof data === 'object' || artifactType === 'result') {
      contentType = 'application/json';
    } else if (typeof data === 'string') {
      contentType = 'text/plain';
    }

    // Upload to Supabase Storage
    const { data: uploadData, error } = await this.supabase.storage
      .from(this.bucketName)
      .upload(fullPath, processedData, {
        contentType: contentType,
        metadata: {
          jobId: jobId,
          artifactType: artifactType,
          compressed: compressed.toString(),
          originalSize: originalSize.toString(),
          ...metadata
        }
      });

    if (error) {
      console.error('Failed to upload to Supabase Storage:', error);
      throw new Error(`Supabase storage upload failed: ${error.message}`);
    }

    const storageUrl = `supabase://${this.bucketName}/${fullPath}`;
    
    console.log(`Successfully stored artifact: ${storageUrl} (${compressedSize} bytes)`);

    return {
      storage_url: storageUrl,
      storage_path: fullPath,
      bucket: this.bucketName,
      artifact_metadata: {
        type: artifactType,
        content_type: contentType,
        original_size: originalSize,
        stored_size: compressedSize,
        compressed: compressed,
        stored_at: new Date().toISOString(),
        supabase_path: uploadData.path
      }
    };
  }

  /**
   * Retrieve artifact from Supabase Storage
   */
  async retrieveArtifact(storageUrl) {
    if (!this.enabled) {
      throw new Error('Supabase storage not enabled');
    }

    // Parse Supabase URL: supabase://bucket/path
    const match = storageUrl.match(/^supabase:\/\/([^\/]+)\/(.+)$/);
    if (!match) {
      throw new Error(`Invalid Supabase URL format: ${storageUrl}`);
    }

    const [, bucket, path] = match;

    console.log(`Retrieving artifact from Supabase: bucket=${bucket}, path=${path}`);

    // Download from Supabase Storage
    const { data, error } = await this.supabase.storage
      .from(bucket)
      .download(path);

    if (error) {
      throw new Error(`Failed to download from Supabase Storage: ${error.message}`);
    }

    // Convert blob to buffer
    const arrayBuffer = await data.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);

    // Check if data is compressed based on path
    const isCompressed = path.endsWith('.gz');
    
    if (isCompressed) {
      const decompressed = await gunzip(buffer);
      return decompressed.toString('utf8');
    }

    // Return as string if it's JSON or text based on path
    if (path.includes('result') || path.endsWith('.json')) {
      return buffer.toString('utf8');
    }

    // Return raw bytes for binary data
    return buffer;
  }

  /**
   * Check if artifact exists in Supabase Storage
   */
  async artifactExists(storageUrl) {
    if (!this.enabled) return false;

    const match = storageUrl.match(/^supabase:\/\/([^\/]+)\/(.+)$/);
    if (!match) return false;

    const [, bucket, path] = match;

    try {
      const { data, error } = await this.supabase.storage
        .from(bucket)
        .list(path.split('/').slice(0, -1).join('/'), {
          search: path.split('/').pop()
        });
      
      if (error) return false;
      
      return data && data.length > 0;
    } catch (error) {
      return false;
    }
  }

  /**
   * Create a truncated version of large result for Kafka
   */
  createResultSummary(originalResult, maxSize = 1024) {
    if (typeof originalResult === 'string') {
      return originalResult.length > maxSize 
        ? originalResult.substring(0, maxSize) + '... [truncated - full result in storage]'
        : originalResult;
    }

    if (Array.isArray(originalResult)) {
      return {
        type: 'array',
        length: originalResult.length,
        first_items: originalResult.slice(0, 3),
        truncated: originalResult.length > 3,
        storage_note: 'Full array stored in Supabase Storage'
      };
    }

    if (typeof originalResult === 'object') {
      const summary = { ...originalResult };
      const summaryStr = JSON.stringify(summary);
      
      if (summaryStr.length > maxSize) {
        return {
          type: 'object',
          keys: Object.keys(originalResult),
          preview: JSON.stringify(originalResult).substring(0, maxSize) + '... [truncated - full object in storage]',
          storage_note: 'Full object stored in Supabase Storage'
        };
      }
      
      return summary;
    }

    return originalResult;
  }

  /**
   * Delete artifact from storage (useful for cleanup)
   */
  async deleteArtifact(storageUrl) {
    if (!this.enabled) return false;

    const match = storageUrl.match(/^supabase:\/\/([^\/]+)\/(.+)$/);
    if (!match) return false;

    const [, bucket, path] = match;

    try {
      const { error } = await this.supabase.storage
        .from(bucket)
        .remove([path]);
      
      if (error) {
        console.error(`Failed to delete artifact: ${error.message}`);
        return false;
      }
      
      console.log(`Successfully deleted artifact: ${storageUrl}`);
      return true;
    } catch (error) {
      console.error(`Error deleting artifact: ${error.message}`);
      return false;
    }
  }
}

module.exports = { SupabaseArtifactStorageClient };