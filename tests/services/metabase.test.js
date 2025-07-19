import { jest } from '@jest/globals';
import { MetabaseService } from '../../src/services/metabase.js';

// Mock axios
const mockAxios = {
  post: jest.fn(),
  get: jest.fn(),
  defaults: {
    headers: {
      common: {}
    }
  }
};

jest.mock('axios', () => mockAxios);

// Mock logger
jest.mock('../../src/utils/logger.js', () => ({
  logger: {
    debug: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn()
  }
}));

import { logger } from '../../src/utils/logger.js';

describe('MetabaseService', () => {
  let service;

  beforeEach(() => {
    service = new MetabaseService();
    jest.clearAllMocks();
    
    // Set up environment variables
    process.env.METABASE_BASE_URL = 'http://localhost:3000';
    process.env.METABASE_DATABASE_ID = '1';
    process.env.DB_BATCH_SIZE = '1000';
  });

  afterEach(() => {
    delete process.env.METABASE_BASE_URL;
    delete process.env.METABASE_DATABASE_ID;
    delete process.env.DB_BATCH_SIZE;
  });

  describe('authenticate', () => {
    test('should authenticate successfully', async () => {
      const mockResponse = {
        data: {
          id: 'session-token-123'
        }
      };
      mockAxios.post.mockResolvedValue(mockResponse);

      const result = await service.authenticate('username', 'password');

      expect(result.success).toBe(true);
      expect(service.sessionToken).toBe('session-token-123');
      expect(service.isAuthenticated).toBe(true);
      expect(mockAxios.post).toHaveBeenCalledWith(
        'http://localhost:3000/api/session',
        {
          username: 'username',
          password: 'password'
        }
      );
    });

    test('should handle authentication failure', async () => {
      const error = new Error('Invalid credentials');
      error.response = { data: { message: 'Invalid credentials' } };
      mockAxios.post.mockRejectedValue(error);

      const result = await service.authenticate('username', 'wrong-password');

      expect(result.success).toBe(false);
      expect(result.error).toBe('Invalid credentials');
      expect(service.sessionToken).toBeNull();
      expect(service.isAuthenticated).toBe(false);
    });

    test('should handle missing base URL', async () => {
      delete process.env.METABASE_BASE_URL;

      const result = await service.authenticate('username', 'password');

      expect(result.success).toBe(false);
      expect(result.error).toContain('undefined');
    });
  });

  describe('getTables', () => {
    beforeEach(() => {
      service.sessionToken = 'valid-token';
      service.isAuthenticated = true;
    });

    test('should get tables successfully', async () => {
      const mockResponse = {
        data: {
          tables: [
            { id: 1, name: 'users', display_name: 'Users', schema: 'public', fields: [] },
            { id: 2, name: 'orders', display_name: 'Orders', schema: 'public', fields: [] }
          ]
        }
      };
      mockAxios.get.mockResolvedValue(mockResponse);

      const result = await service.getTables();

      expect(result.success).toBe(true);
      expect(result.tables).toHaveLength(2);
      expect(result.tables[0].name).toBe('users');
      expect(mockAxios.get).toHaveBeenCalledWith(
        'http://localhost:3000/api/database/1/metadata',
        { headers: { 'X-Metabase-Session': 'valid-token' } }
      );
    });

    test('should handle API error', async () => {
      const error = new Error('API Error');
      error.response = { data: { message: 'API Error' } };
      mockAxios.get.mockRejectedValue(error);

      const result = await service.getTables();

      expect(result.success).toBe(false);
      expect(result.error).toBe('API Error');
    });

    test('should handle missing session token', async () => {
      service.isAuthenticated = false;

      await expect(service.getTables()).rejects.toThrow('Not authenticated');
    });
  });

  describe('getTableRowCount', () => {
    beforeEach(() => {
      service.sessionToken = 'valid-token';
      service.isAuthenticated = true;
    });

    test('should get table row count successfully', async () => {
      const mockResponse = {
        data: {
          data: {
            rows: [[1000]]
          }
        }
      };
      mockAxios.post.mockResolvedValue(mockResponse);

      const result = await service.getTableRowCount(1);

      expect(result.success).toBe(true);
      expect(result.count).toBe(1000);
      expect(mockAxios.post).toHaveBeenCalledWith(
        'http://localhost:3000/api/dataset',
        {
          database: 1,
          query: {
            'source-table': 1,
            aggregation: [['count']]
          },
          type: 'query'
        }
      );
    });

    test('should handle empty result', async () => {
      const mockResponse = {
        data: {
          data: {
            rows: []
          }
        }
      };
      mockAxios.post.mockResolvedValue(mockResponse);

      const result = await service.getTableRowCount(1);

      expect(result.success).toBe(true);
      expect(result.count).toBe(0);
    });

    test('should handle API error', async () => {
      mockAxios.post.mockRejectedValue(new Error('Query failed'));

      const result = await service.getTableRowCount(1);

      expect(result.success).toBe(false);
      expect(result.error).toBe('Query failed');
    });
  });

  describe('queryTable', () => {
    beforeEach(() => {
      service.sessionToken = 'valid-token';
      service.isAuthenticated = true;
    });

    test('should query table data successfully', async () => {
      const mockResponse = {
        data: {
          data: {
            rows: [
              [1, 'John', 'john@example.com'],
              [2, 'Jane', 'jane@example.com']
            ],
            cols: [
              { name: 'id', display_name: 'ID', base_type: 'type/Integer' },
              { name: 'name', display_name: 'Name', base_type: 'type/Text' },
              { name: 'email', display_name: 'Email', base_type: 'type/Text' }
            ]
          }
        }
      };
      mockAxios.post.mockResolvedValue(mockResponse);

      const result = await service.queryTable(1);

      expect(result.success).toBe(true);
      expect(result.data.rows).toHaveLength(2);
      expect(result.data.rows[0]).toEqual({
        id: 1,
        name: 'John',
        email: 'john@example.com'
      });
    });

    test('should handle pagination with limit and offset', async () => {
      const mockResponse = {
        data: {
          data: {
            rows: [[1, 'John']],
            cols: [
              { name: 'id', display_name: 'ID', base_type: 'type/Integer' },
              { name: 'name', display_name: 'Name', base_type: 'type/Text' }
            ]
          }
        }
      };
      mockAxios.post.mockResolvedValue(mockResponse);

      const result = await service.queryTable(1, { limit: 10, offset: 20 });

      expect(result.success).toBe(true);
      expect(mockAxios.post).toHaveBeenCalledWith(
        'http://localhost:3000/api/dataset',
        expect.objectContaining({
          query: expect.objectContaining({
            limit: 10,
            page: { page: 2, items: 10 }
          })
        }),
        expect.objectContaining({
          headers: expect.objectContaining({
            'X-Metabase-Session': 'valid-token'
          })
        })
      );
    });

    test('should handle empty table', async () => {
      const mockResponse = {
        data: {
          data: {
            rows: [],
            cols: [{ name: 'id', display_name: 'ID', base_type: 'type/Integer' }]
          }
        }
      };
      mockAxios.post.mockResolvedValue(mockResponse);

      const result = await service.queryTable(1);

      expect(result.success).toBe(true);
      expect(result.data.rows).toEqual([]);
    });

    test('should handle API error', async () => {
      const error = new Error('Query failed');
      error.response = { data: { message: 'Query failed' } };
      mockAxios.post.mockRejectedValue(error);

      const result = await service.queryTable(1);

      expect(result.success).toBe(false);
      expect(result.error).toBe('Query failed');
    });
  });

  describe('extractAllTableData', () => {
    beforeEach(() => {
      service.sessionToken = 'valid-token';
      service.isAuthenticated = true;
    });

    test('should extract all data successfully', async () => {
      // Mock getTableRowCount
      const countResponse = {
        data: {
          data: {
            rows: [[100]]
          }
        }
      };

      // Mock queryTable
      const queryResponse = {
        data: {
          data: {
            rows: [
              [1, 'John'],
              [2, 'Jane']
            ],
            cols: [
              { name: 'id', display_name: 'ID', base_type: 'type/Integer' },
              { name: 'name', display_name: 'Name', base_type: 'type/Text' }
            ]
          }
        }
      };

      mockAxios.post
        .mockResolvedValueOnce(countResponse)  // getTableRowCount call
        .mockResolvedValueOnce(queryResponse); // queryTable call

      const result = await service.extractAllTableData(1, 'users');

      expect(result.success).toBe(true);
      expect(result.data).toHaveLength(2);
      expect(result.tableName).toBe('users');
      expect(result.totalRows).toBe(100);
      expect(result.extractedRows).toBe(2);
    });

    test('should handle row count error', async () => {
      const error = new Error('Count failed');
      error.response = { data: { message: 'Count failed' } };
      mockAxios.post.mockRejectedValue(error);

      const result = await service.extractAllTableData(1, 'users');

      expect(result.success).toBe(false);
      expect(result.error).toContain('Failed to get row count');
    });

    test('should handle query error', async () => {
      // Mock successful count
      const countResponse = {
        data: {
          data: {
            rows: [[100]]
          }
        }
      };

      mockAxios.post
        .mockResolvedValueOnce(countResponse)
        .mockRejectedValue(new Error('Query failed'));

      const result = await service.extractAllTableData(1, 'users');

      expect(result.success).toBe(false);
      expect(result.error).toContain('Batch extraction failed');
    });
  });

  describe('logout', () => {
    test('should logout successfully', async () => {
      service.sessionToken = 'valid-token';
      mockAxios.delete = jest.fn().mockResolvedValue({ data: {} });

      await service.logout();

      expect(service.sessionToken).toBeNull();
      expect(service.isAuthenticated).toBe(false);
      expect(mockAxios.delete).toHaveBeenCalledWith(
        'http://localhost:3000/api/session',
        { headers: { 'X-Metabase-Session': 'valid-token' } }
      );
    });

    test('should handle logout error gracefully', async () => {
      service.sessionToken = 'valid-token';
      mockAxios.delete = jest.fn().mockRejectedValue(new Error('Logout failed'));

      await service.logout();

      expect(service.sessionToken).toBeNull();
      expect(service.isAuthenticated).toBe(false);
    });

    test('should handle logout when not authenticated', async () => {
      service.sessionToken = null;
      mockAxios.delete = jest.fn();

      await service.logout();

      expect(mockAxios.delete).not.toHaveBeenCalled();
    });
  });

  describe('getDatabaseInfo', () => {
    beforeEach(() => {
      service.sessionToken = 'valid-token';
      service.isAuthenticated = true;
    });

    test('should get database info successfully', async () => {
      const mockResponse = {
        data: {
          id: 1,
          name: 'Test Database',
          engine: 'postgres'
        }
      };
      mockAxios.get.mockResolvedValue(mockResponse);

      const result = await service.getDatabaseInfo();

      expect(result.success).toBe(true);
      expect(result.database.name).toBe('Test Database');
      expect(mockAxios.get).toHaveBeenCalledWith(
        'http://localhost:3000/api/database/1',
        { headers: { 'X-Metabase-Session': 'valid-token' } }
      );
    });

    test('should handle API error', async () => {
      const error = new Error('Database not found');
      error.response = { data: { message: 'Database not found' } };
      mockAxios.get.mockRejectedValue(error);

      const result = await service.getDatabaseInfo();

      expect(result.success).toBe(false);
      expect(result.error).toBe('Database not found');
    });

    test('should handle missing authentication', async () => {
      service.isAuthenticated = false;

      await expect(service.getDatabaseInfo()).rejects.toThrow('Not authenticated');
    });
  });
});