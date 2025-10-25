import {
  S3Client,
  GetObjectCommand,
  ListObjectsV2Command,
} from "@aws-sdk/client-s3";

interface SnapshotMetadata {
  filename: string;
  blake2_hash: string;
  url: string;
  stage: string;
  saved_at: string;
  file_size: number;
  parsed_data?: any;
}

interface StorageBackend {
  readHtmlSnapshot(url: string, stage: string): Promise<string | null>;
  readMetadata(s3Path: string): Promise<Record<string, any>>;
  findMostRecentSnapshot(
    url: string,
    stage: string,
  ): Promise<{ html: string; metadata: SnapshotMetadata } | null>;
}

export class S3StorageBackend implements StorageBackend {
  private s3Client: S3Client;
  private bucketName: string;
  private metadataCache: Map<string, Record<string, any>> = new Map();

  constructor() {
    const endpoint = process.env.DIGITALOCEAN_S3_ENDPOINT;
    const accessKeyId = process.env.DIGITALOCEAN_S3_ACCESS_KEY;
    const secretAccessKey = process.env.DIGITALOCEAN_S3_SECRET_KEY;
    const region = process.env.DIGITALOCEAN_S3_REGION;
    const bucketName = process.env.DOKITO_S3_HTML_BUCKET;

    // Validate ALL required S3 configuration
    const missing: string[] = [];
    if (!endpoint) missing.push("DIGITALOCEAN_S3_ENDPOINT");
    if (!accessKeyId) missing.push("DIGITALOCEAN_S3_ACCESS_KEY");
    if (!secretAccessKey) missing.push("DIGITALOCEAN_S3_SECRET_KEY");
    if (!region) missing.push("DIGITALOCEAN_S3_REGION");
    if (!bucketName) missing.push("DOKITO_S3_HTML_BUCKET");

    if (missing.length > 0) {
      throw new Error(
        `S3 configuration incomplete. Missing required environment variables: ${missing.join(", ")}`,
      );
    }

    this.bucketName = bucketName!;

    this.s3Client = new S3Client({
      region: region!,
      endpoint: endpoint!,
      credentials: {
        accessKeyId: accessKeyId!,
        secretAccessKey: secretAccessKey!,
      },
      forcePathStyle: true, // Required for DigitalOcean Spaces and MinIO
    });

    console.error(
      `S3 storage initialized: bucket=${this.bucketName}, endpoint=${endpoint}, region=${region}`,
    );
  }

  private urlToS3Path(url: string): string {
    const urlObj = new URL(url);
    const hostname = urlObj.hostname;
    const pathname = urlObj.pathname;
    const pathParts = pathname.split("/").filter((part) => part.length > 0);
    pathParts.pop(); // Remove filename
    const directoryPath = pathParts.join("/");

    return `raw/ny/puc/${hostname}/${directoryPath}`;
  }

  async readMetadata(s3DirectoryPath: string): Promise<Record<string, any>> {
    // Check cache first
    if (this.metadataCache.has(s3DirectoryPath)) {
      return this.metadataCache.get(s3DirectoryPath)!;
    }

    const metadataKey = `${s3DirectoryPath}/metadata.json`;

    try {
      const command = new GetObjectCommand({
        Bucket: this.bucketName,
        Key: metadataKey,
      });

      const response = await this.s3Client.send(command);
      const bodyString = await response.Body!.transformToString();
      const metadata = JSON.parse(bodyString);

      // Cache it
      this.metadataCache.set(s3DirectoryPath, metadata);

      return metadata;
    } catch (error: any) {
      if (error.name === "NoSuchKey") {
        console.error(
          `No metadata.json found at s3://${this.bucketName}/${metadataKey}`,
        );
        return {};
      }
      throw error;
    }
  }

  async findMostRecentSnapshot(
    url: string,
    stage: string,
  ): Promise<{ html: string; metadata: SnapshotMetadata } | null> {
    const s3Path = this.urlToS3Path(url);
    const metadata = await this.readMetadata(s3Path);

    // Find most recent snapshot matching url and stage
    let mostRecent: { filename: string; entry: SnapshotMetadata } | null = null;
    let latestTimestamp: string | null = null;

    for (const [filename, entry] of Object.entries(metadata)) {
      if (entry.url === url && entry.stage === stage) {
        if (!latestTimestamp || entry.saved_at > latestTimestamp) {
          mostRecent = { filename, entry: entry as SnapshotMetadata };
          latestTimestamp = entry.saved_at;
        }
      }
    }

    if (!mostRecent) {
      console.error(
        `No snapshot found in S3 for url=${url}, stage=${stage} in path=${s3Path}`,
      );
      return null;
    }

    // Read the HTML file from S3
    const htmlKey = `${s3Path}/${mostRecent.filename}`;
    try {
      const command = new GetObjectCommand({
        Bucket: this.bucketName,
        Key: htmlKey,
      });

      const response = await this.s3Client.send(command);
      const html = await response.Body!.transformToString();

      console.error(
        `Successfully loaded HTML from S3: ${htmlKey} (${mostRecent.entry.file_size} bytes)`,
      );

      return { html, metadata: mostRecent.entry };
    } catch (error: any) {
      if (error.name === "NoSuchKey") {
        console.error(
          `HTML file not found in S3: s3://${this.bucketName}/${htmlKey}`,
        );
        return null;
      }
      throw error;
    }
  }

  async readHtmlSnapshot(url: string, stage: string): Promise<string | null> {
    const result = await this.findMostRecentSnapshot(url, stage);
    return result?.html || null;
  }
}
