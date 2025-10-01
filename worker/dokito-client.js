const axios = require('axios');

class DokitoClient {
  constructor(baseUrl, logger) {
    this.baseUrl = baseUrl;
    this.logger = logger;
    
    // Create axios instance with default configuration
    this.client = axios.create({
      baseURL: baseUrl,
      timeout: 30000,
      headers: {
        'Content-Type': 'application/json',
      },
    });

    // Add request interceptor for logging
    this.client.interceptors.request.use(
      (config) => {
        this.logger.debug(`Making ${config.method.toUpperCase()} request to ${config.url}`, {
          headers: config.headers,
          params: config.params,
        });
        return config;
      },
      (error) => {
        this.logger.error('Request error', { error: error.message });
        return Promise.reject(error);
      }
    );

    // Add response interceptor for logging
    this.client.interceptors.response.use(
      (response) => {
        this.logger.debug(`Received response from ${response.config.url}`, {
          status: response.status,
          statusText: response.statusText,
        });
        return response;
      },
      (error) => {
        const errorDetails = {
          status: error.response?.status,
          statusText: error.response?.statusText,
          data: error.response?.data,
          message: error.message,
        };
        this.logger.error('Response error', errorDetails);
        return Promise.reject(error);
      }
    );
  }

  /**
   * Fetch processed case filing data
   * @param {string} state - The state of the jurisdiction
   * @param {string} jurisdictionName - The name of the jurisdiction
   * @param {string} caseName - The name of the case
   * @returns {Promise<Object>} Case data
   */
  async fetchCase(state, jurisdictionName, caseName) {
    const url = `/public/cases/${encodeURIComponent(state)}/${encodeURIComponent(jurisdictionName)}/${encodeURIComponent(caseName)}`;
    const response = await this.client.get(url);
    return response.data;
  }

  /**
   * List all cases for a jurisdiction with pagination
   * @param {string} state - The state of the jurisdiction
   * @param {string} jurisdictionName - The name of the jurisdiction
   * @param {number} limit - Number of items to return
   * @param {number} offset - Number of items to skip
   * @returns {Promise<Object>} Case list with pagination info
   */
  async fetchCaseList(state, jurisdictionName, limit = 10, offset = 0) {
    const url = `/public/caselist/${encodeURIComponent(state)}/${encodeURIComponent(jurisdictionName)}/all`;
    const params = { limit, offset };
    const response = await this.client.get(url, { params });
    return response.data;
  }

  /**
   * Fetch attachment metadata
   * @param {string} blake2bHash - The blake2b hash of the attachment
   * @returns {Promise<Object>} Attachment metadata
   */
  async fetchAttachmentObj(blake2bHash) {
    const url = `/public/raw_attachments/${encodeURIComponent(blake2bHash)}/obj`;
    const response = await this.client.get(url);
    return response.data;
  }

  /**
   * Fetch raw attachment file content
   * @param {string} blake2bHash - The blake2b hash of the attachment
   * @returns {Promise<Buffer|String>} Raw attachment content
   */
  async fetchAttachmentRaw(blake2bHash) {
    const url = `/public/raw_attachments/${encodeURIComponent(blake2bHash)}/raw`;
    const response = await this.client.get(url, {
      responseType: 'arraybuffer', // Handle binary data
    });
    return response.data;
  }

  /**
   * Submit case data for processing
   * @param {Object} caseData - Case data to submit
   * @returns {Promise<Object>} Submission result
   */
  async submitCase(caseData) {
    const url = '/admin/cases/submit';
    const response = await this.client.post(url, caseData);
    return response.data;
  }

  /**
   * Trigger reprocessing operations
   * @param {string} operationType - Type of operation (dockets, hashes-random, hashes-newest, purge)
   * @param {string} state - State (required for purge operation)
   * @param {string} jurisdictionName - Jurisdiction name (required for purge operation)
   * @returns {Promise<Object>} Operation result
   */
  async reprocessOperation(operationType, state = null, jurisdictionName = null) {
    let url;
    let method = 'POST';
    
    switch (operationType) {
      case 'dockets':
        url = '/admin/cases/reprocess_dockets_for_all';
        break;
      case 'hashes-random':
        url = '/admin/cases/download_missing_hashes_for_all/random';
        break;
      case 'hashes-newest':
        url = '/admin/cases/download_missing_hashes_for_all/newest';
        break;
      case 'purge':
        if (!state || !jurisdictionName) {
          throw new Error('State and jurisdiction name are required for purge operation');
        }
        url = `/admin/cases/${encodeURIComponent(state)}/${encodeURIComponent(jurisdictionName)}/purge_all`;
        method = 'DELETE';
        break;
      default:
        throw new Error(`Unsupported operation type: ${operationType}`);
    }

    const response = await this.client.request({
      method,
      url,
    });
    return response.data;
  }

  /**
   * Get case data differential
   * @param {string} state - The state of the jurisdiction
   * @param {string} jurisdictionName - The name of the jurisdiction
   * @param {Object} payload - Differential request payload
   * @returns {Promise<Object>} Differential data
   */
  async getCaseDataDifferential(state, jurisdictionName, payload) {
    const url = `/public/caselist/${encodeURIComponent(state)}/${encodeURIComponent(jurisdictionName)}/casedata_differential`;
    const response = await this.client.post(url, payload);
    return response.data;
  }

  /**
   * Read arbitrary file from S3
   * @param {string} path - S3 file path
   * @returns {Promise<Object>} File content
   */
  async readS3File(path) {
    const url = `/public/read_openscrapers_s3_file/${encodeURIComponent(path)}`;
    const response = await this.client.get(url);
    return response.data;
  }

  /**
   * Health check
   * @returns {Promise<Object>} Health status
   */
  async healthCheck() {
    const response = await this.client.get('/health');
    return response.data;
  }
}

module.exports = { DokitoClient };