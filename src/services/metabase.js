import axios from 'axios';
import { logger } from '../utils/logger.js';

class MetabaseService {
  constructor() {
    this.sessionToken = null;
    this.isAuthenticated = false;
  }

  get baseURL() {
    return process.env.METABASE_BASE_URL;
  }

  get databaseId() {
    return parseInt(process.env.METABASE_DATABASE_ID);
  }

  async authenticate(username, password) {
    try {
      const response = await axios.post(`${this.baseURL}/api/session`, {
        username,
        password
      });

      this.sessionToken = response.data.id;
      this.isAuthenticated = true;

      return {
        success: true,
        sessionToken: this.sessionToken
      };
    } catch (error) {
      return {
        success: false,
        error: error.response?.data?.message || error.message
      };
    }
  }

  async getDatabaseInfo() {
    if (!this.isAuthenticated) {
      throw new Error('Not authenticated. Call authenticate() first.');
    }

    try {
      const response = await axios.get(
        `${this.baseURL}/api/database/${this.databaseId}`,
        {
          headers: {
            'X-Metabase-Session': this.sessionToken
          }
        }
      );

      return {
        success: true,
        database: response.data
      };
    } catch (error) {
      return {
        success: false,
        error: error.response?.data?.message || error.message
      };
    }
  }

  async getTables() {
    if (!this.isAuthenticated) {
      throw new Error('Not authenticated. Call authenticate() first.');
    }

    try {
      const response = await axios.get(
        `${this.baseURL}/api/database/${this.databaseId}/metadata`,
        {
          headers: {
            'X-Metabase-Session': this.sessionToken
          }
        }
      );

      const tables = response.data.tables || [];
      return {
        success: true,
        tables: tables.map((table) => ({
          id: table.id,
          name: table.name,
          display_name: table.display_name,
          schema: table.schema,
          fields:
            table.fields?.map((field) => ({
              id: field.id,
              name: field.name,
              display_name: field.display_name,
              base_type: field.base_type,
              semantic_type: field.semantic_type
            })) || []
        }))
      };
    } catch (error) {
      return {
        success: false,
        error: error.response?.data?.message || error.message
      };
    }
  }

  async queryTable(tableId, options = {}) {
    if (!this.isAuthenticated) {
      throw new Error('Not authenticated. Call authenticate() first.');
    }

    const { limit = parseInt(process.env.DB_BATCH_SIZE) || 1000, offset = 0 } =
      options;

    try {
      const query = {
        database: this.databaseId,
        type: 'query',
        query: {
          'source-table': tableId,
          limit: limit
        }
      };

      if (offset > 0) {
        query.query.page = {
          page: Math.floor(offset / limit),
          items: limit
        };
      }

      const response = await axios.post(`${this.baseURL}/api/dataset`, query, {
        headers: {
          'X-Metabase-Session': this.sessionToken,
          'Content-Type': 'application/json'
        }
      });

      const data = response.data;

      const columns = data.data.cols.map((col) => ({
        name: col.name,
        display_name: col.display_name,
        base_type: col.base_type
      }));

      const rows = data.data.rows.map((row) => {
        const rowObj = {};
        columns.forEach((col, index) => {
          rowObj[col.name] = row[index];
        });
        return rowObj;
      });

      return {
        success: true,
        data: {
          columns,
          rows,
          row_count: data.row_count || rows.length,
          total_rows: data.data.rows_truncated ? 'unknown' : rows.length
        }
      };
    } catch (error) {
      return {
        success: false,
        error: error.response?.data?.message || error.message,
        details: error.response?.data
      };
    }
  }

  async getTableRowCount(tableId) {
    if (!this.isAuthenticated) {
      throw new Error('Not authenticated. Call authenticate() first.');
    }

    try {
      const query = {
        database: this.databaseId,
        type: 'query',
        query: {
          'source-table': tableId,
          aggregation: [['count']]
        }
      };

      const response = await axios.post(`${this.baseURL}/api/dataset`, query, {
        headers: {
          'X-Metabase-Session': this.sessionToken,
          'Content-Type': 'application/json'
        }
      });

      const count = response.data.data.rows[0][0];
      return {
        success: true,
        count: parseInt(count)
      };
    } catch (error) {
      return {
        success: false,
        error: error.response?.data?.message || error.message,
        count: 0
      };
    }
  }

  async extractAllTableData(tableId, tableName, onBatch = null) {
    if (!this.isAuthenticated) {
      throw new Error('Not authenticated. Call authenticate() first.');
    }

    try {
      const countResult = await this.getTableRowCount(tableId);
      if (!countResult.success) {
        throw new Error(`Failed to get row count: ${countResult.error}`);
      }

      const totalRows = countResult.count;
      const batchSize = parseInt(process.env.DB_BATCH_SIZE) || 1000;
      let allData = [];
      let extractedRows = 0;

      for (let offset = 0; offset < totalRows; offset += batchSize) {
        const batchResult = await this.queryTable(tableId, {
          limit: batchSize,
          offset
        });

        if (!batchResult.success) {
          throw new Error(`Batch extraction failed: ${batchResult.error}`);
        }

        const batchData = batchResult.data.rows;
        allData = allData.concat(batchData);
        extractedRows += batchData.length;

        if (onBatch && typeof onBatch === 'function') {
          await onBatch({
            tableName,
            batchData,
            batchNumber: Math.floor(offset / batchSize) + 1,
            totalExtracted: extractedRows,
            totalRows,
            progress: (extractedRows / totalRows) * 100
          });
        }

        if (batchData.length < batchSize) {
          break;
        }
      }

      return {
        success: true,
        tableName,
        data: allData,
        totalRows,
        extractedRows,
        columns: allData.length > 0 ? Object.keys(allData[0]) : []
      };
    } catch (error) {
      return {
        success: false,
        error: error.message,
        tableName,
        data: [],
        totalRows: 0,
        extractedRows: 0
      };
    }
  }

  async logout() {
    if (this.sessionToken) {
      try {
        await axios.delete(`${this.baseURL}/api/session`, {
          headers: {
            'X-Metabase-Session': this.sessionToken
          }
        });
        logger.debug('Logged out from Metabase');
      } catch {
        logger.debug('Logout error (session may have expired)');
      }
    }

    this.sessionToken = null;
    this.isAuthenticated = false;
  }
}

const metabaseService = new MetabaseService();

export { MetabaseService, metabaseService };
