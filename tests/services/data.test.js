import { jest } from '@jest/globals';

import { DataService, dataService } from '../../src/services/data.js';
import { connectionService } from '../../src/services/connection.js';

describe('Data Service', () => {
  let originalEnv;

  beforeEach(() => {
    originalEnv = { ...process.env };

    process.env.DB_BATCH_SIZE = '100';

    jest.clearAllMocks();

    jest.spyOn(connectionService, 'initialize').mockResolvedValue({ success: true });
    connectionService.isInitialized = true;
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  describe('DataService', () => {
    test('should initialize successfully', async () => {
      const service = new DataService();
      const result = await service.initialize();

      expect(result.success).toBe(true);
      expect(result.message).toBe('Data service initialized successfully');
      expect(service.isInitialized).toBe(true);
    });

    test('should fail initialization when connection service fails', async () => {
      connectionService.isInitialized = false;
      connectionService.initialize.mockResolvedValue({
        success: false,
        details: ['Connection failed']
      });

      const service = new DataService();
      const result = await service.initialize();

      expect(result.success).toBe(false);
      expect(result.error).toBe('Failed to initialize connection service');
    });

    test('should get table names from database', async () => {
      const service = new DataService();
      await service.initialize();

      const mockConnection = {
        query: jest.fn().mockResolvedValue({
          rows: [
            { table_name: 'users' },
            { table_name: 'orders' },
            { table_name: 'products' }
          ]
        })
      };

      const tableNames = await service.getTableNames(mockConnection);

      expect(tableNames).toEqual(['users', 'orders', 'products']);
      expect(mockConnection.query).toHaveBeenCalledWith(expect.stringContaining('information_schema.tables'));
    });

    test('should handle error when getting table names', async () => {
      const service = new DataService();
      await service.initialize();

      const mockConnection = {
        query: jest.fn().mockRejectedValue(new Error('Database error'))
      };

      await expect(service.getTableNames(mockConnection)).rejects.toThrow('Failed to get table names: Database error');
    });

    test('should get table dependencies', async () => {
      const service = new DataService();
      await service.initialize();

      const mockConnection = {
        query: jest.fn().mockResolvedValue({
          rows: [
            { dependent_table: 'orders', referenced_table: 'users' },
            { dependent_table: 'order_items', referenced_table: 'orders' },
            { dependent_table: 'order_items', referenced_table: 'products' }
          ]
        })
      };

      const dependencies = await service.getTableDependencies(mockConnection);

      expect(dependencies).toEqual({
        orders: ['users'],
        order_items: ['orders', 'products']
      });
    });

    test('should sort tables by dependencies', () => {
      const service = new DataService();
      const tableNames = ['order_items', 'orders', 'users', 'products'];
      const dependencies = {
        orders: ['users'],
        order_items: ['orders', 'products']
      };

      const sorted = service.sortTablesByDependencies(tableNames, dependencies);

      expect(sorted.indexOf('users')).toBeLessThan(sorted.indexOf('orders'));
      expect(sorted.indexOf('products')).toBeLessThan(sorted.indexOf('order_items'));
      expect(sorted.indexOf('orders')).toBeLessThan(sorted.indexOf('order_items'));
    });

    test('should handle circular dependencies gracefully', () => {
      const service = new DataService();
      const tableNames = ['table_a', 'table_b'];
      const dependencies = {
        table_a: ['table_b'],
        table_b: ['table_a']
      };

      const sorted = service.sortTablesByDependencies(tableNames, dependencies);

      expect(sorted).toHaveLength(2);
      expect(sorted).toContain('table_a');
      expect(sorted).toContain('table_b');
    });

    test('should get table row count', async () => {
      const service = new DataService();
      await service.initialize();

      const mockConnection = {
        query: jest.fn().mockResolvedValue({
          rows: [{ count: '150' }]
        })
      };

      const count = await service.getTableRowCount(mockConnection, 'users');

      expect(count).toBe(150);
      expect(mockConnection.query).toHaveBeenCalledWith('SELECT COUNT(*) as count FROM "users";');
    });

    test('should extract table data with batching', async () => {
      const service = new DataService();
      await service.initialize();

      const mockConnection = {
        query: jest.fn()
          .mockResolvedValueOnce({ rows: [{ count: '250' }] }) 
          .mockResolvedValueOnce({ 
            rows: [
              { id: 1, name: 'User 1' },
              { id: 2, name: 'User 2' }
            ],
            fields: [{ name: 'id' }, { name: 'name' }]
          })
      };

      const result = await service.extractTableData(mockConnection, 'users', { offset: 0, limit: 2 });

      expect(result).toEqual({
        tableName: 'users',
        data: [
          { id: 1, name: 'User 1' },
          { id: 2, name: 'User 2' }
        ],
        totalRows: 250,
        extractedRows: 2,
        hasMore: true,
        nextOffset: 2,
        columns: ['id', 'name']
      });
    });

    test('should handle empty table extraction', async () => {
      const service = new DataService();
      await service.initialize();

      const mockConnection = {
        query: jest.fn().mockResolvedValue({ rows: [{ count: '0' }] })
      };

      const result = await service.extractTableData(mockConnection, 'empty_table');

      expect(result).toEqual({
        tableName: 'empty_table',
        data: [],
        totalRows: 0,
        extractedRows: 0,
        hasMore: false,
        nextOffset: 0
      });
    });

    test('should extract all table data with batching', async () => {
      const service = new DataService();
      service.configureBatchSize(2); 
      await service.initialize();

      const mockConnection = {
        query: jest.fn()
          .mockResolvedValueOnce({ rows: [{ count: '3' }] }) 
          .mockResolvedValueOnce({ 
            rows: [{ id: 1 }, { id: 2 }],
            fields: [{ name: 'id' }]
          })
          .mockResolvedValueOnce({ rows: [{ count: '3' }] }) 
          .mockResolvedValueOnce({ 
            rows: [{ id: 3 }],
            fields: [{ name: 'id' }]
          })
      };

      const batchCallback = jest.fn();
      const result = await service.extractAllTableData(mockConnection, 'test_table', batchCallback);

      expect(result.tableName).toBe('test_table');
      expect(result.data).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
      expect(result.totalRows).toBe(3);
      expect(result.extractedRows).toBe(3);
      expect(result.batchCount).toBe(2);
      expect(batchCallback).toHaveBeenCalledTimes(2);
    });

    test('should extract all data from multiple tables', async () => {
      const service = new DataService();
      await service.initialize();

      const mockConnection = {
        query: jest.fn()
          
          .mockResolvedValueOnce({
            rows: [{ table_name: 'users' }, { table_name: 'orders' }]
          })
          
          .mockResolvedValueOnce({
            rows: [{ dependent_table: 'orders', referenced_table: 'users' }]
          })
          
          .mockResolvedValueOnce({ rows: [{ count: '2' }] })
          
          .mockResolvedValueOnce({
            rows: [{ id: 1, name: 'User 1' }, { id: 2, name: 'User 2' }],
            fields: [{ name: 'id' }, { name: 'name' }]
          })
          
          .mockResolvedValueOnce({ rows: [{ count: '1' }] })
          
          .mockResolvedValueOnce({
            rows: [{ id: 1, user_id: 1, total: 100 }],
            fields: [{ name: 'id' }, { name: 'user_id' }, { name: 'total' }]
          })
      };

      const onTableStart = jest.fn();
      const onTableComplete = jest.fn();
      const onBatch = jest.fn();

      const result = await service.extractAllData(mockConnection, onTableStart, onTableComplete, onBatch);

      expect(result.success).toBe(true);
      expect(result.totalTables).toBe(2);
      expect(result.totalRows).toBe(3);
      expect(result.tableOrder).toEqual(['users', 'orders']); 
      expect(onTableStart).toHaveBeenCalledTimes(2);
      expect(onTableComplete).toHaveBeenCalledTimes(2);
    });

    test('should handle errors during table extraction and continue', async () => {
      const service = new DataService();
      await service.initialize();

      const mockConnection = {
        query: jest.fn()
          
          .mockResolvedValueOnce({
            rows: [{ table_name: 'users' }, { table_name: 'bad_table' }]
          })
          
          .mockResolvedValueOnce({ rows: [] })
          
          .mockResolvedValueOnce({ rows: [{ count: '1' }] })
          
          .mockResolvedValueOnce({
            rows: [{ id: 1, name: 'User 1' }],
            fields: [{ name: 'id' }, { name: 'name' }]
          })
          .mockRejectedValueOnce(new Error('Table does not exist'))
      };

      const result = await service.extractAllData(mockConnection);

      expect(result.success).toBe(false);
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0].tableName).toBe('bad_table');
      expect(result.errors[0].error).toContain('Table does not exist');
      expect(result.tables).toHaveLength(1); 
    });

    test('should handle no tables found', async () => {
      const service = new DataService();
      await service.initialize();

      const mockConnection = {
        query: jest.fn().mockResolvedValue({ rows: [] })
      };

      const result = await service.extractAllData(mockConnection);

      expect(result.success).toBe(true);
      expect(result.totalTables).toBe(0);
      expect(result.totalRows).toBe(0);
      expect(result.message).toBe('No tables found to extract');
    });

    test('should throw error when not initialized', async () => {
      const service = new DataService();
      const mockConnection = { query: jest.fn() };

      await expect(service.extractTableData(mockConnection, 'users')).rejects.toThrow('Data service not initialized');
      await expect(service.extractAllTableData(mockConnection, 'users')).rejects.toThrow('Data service not initialized');
      await expect(service.extractAllData(mockConnection)).rejects.toThrow('Data service not initialized');
    });

    test('should get extraction statistics', () => {
      const service = new DataService();
      const stats = service.getExtractionStats();

      expect(stats.batchSize).toBe(100); 
      expect(stats.initialized).toBe(false);
    });

    test('should configure batch size', () => {
      const service = new DataService();
      service.configureBatchSize(500);

      expect(service.batchSize).toBe(500);

      
      service.configureBatchSize(0);
      expect(service.batchSize).toBe(500);

      service.configureBatchSize(-10);
      expect(service.batchSize).toBe(500);
    });

    test('should use default batch size when env variable is invalid', () => {
      delete process.env.DB_BATCH_SIZE;
      const service = new DataService();

      expect(service.batchSize).toBe(1000); 
    });

    test('should handle extraction error gracefully', async () => {
      const service = new DataService();
      await service.initialize();

      const mockConnection = {
        query: jest.fn().mockRejectedValue(new Error('Connection lost'))
      };

      await expect(service.extractTableData(mockConnection, 'users')).rejects.toThrow('Failed to extract data from table users: Failed to get row count for table users: Connection lost');
    });
  });

  describe('dataService singleton', () => {
    test('should be an instance of DataService', () => {
      expect(dataService).toBeInstanceOf(DataService);
    });

    test('should maintain state across imports', async () => {
      const result = await dataService.initialize();

      expect(result.success).toBe(true);
      expect(dataService.isInitialized).toBe(true);
    });
  });

  describe('Data Insertion', () => {
    test('should clear table data', async () => {
      const service = new DataService();
      await service.initialize();

      const mockConnection = {
        query: jest.fn().mockResolvedValue({ rowCount: 5 })
      };

      const result = await service.clearTableData(mockConnection, 'users');

      expect(result).toEqual({
        tableName: 'users',
        deletedRows: 5,
        success: true
      });
      expect(mockConnection.query).toHaveBeenCalledWith('DELETE FROM "users";');
    });

    test('should get table columns', async () => {
      const service = new DataService();
      await service.initialize();

      const mockConnection = {
        query: jest.fn().mockResolvedValue({
          rows: [
            { column_name: 'id', data_type: 'integer', is_nullable: 'NO', column_default: 'nextval(...)' },
            { column_name: 'name', data_type: 'character varying', is_nullable: 'YES', column_default: null }
          ]
        })
      };

      const columns = await service.getTableColumns(mockConnection, 'users');

      expect(columns).toHaveLength(2);
      expect(columns[0].column_name).toBe('id');
      expect(columns[1].column_name).toBe('name');
    });

    test('should insert table data successfully', async () => {
      const service = new DataService();
      service.configureBatchSize(2); 
      await service.initialize();

      const mockConnection = {
        query: jest.fn()
          
          .mockResolvedValueOnce({
            rows: [
              { column_name: 'id', data_type: 'integer' },
              { column_name: 'name', data_type: 'varchar' }
            ]
          })
          
          .mockResolvedValueOnce({ rowCount: 2 })
          .mockResolvedValueOnce({ rowCount: 1 })
      };

      const testData = [
        { id: 1, name: 'User 1' },
        { id: 2, name: 'User 2' },
        { id: 3, name: 'User 3' }
      ];

      const result = await service.insertTableData(mockConnection, 'users', testData);

      expect(result.success).toBe(true);
      expect(result.insertedRows).toBe(3);
      expect(result.totalRows).toBe(3);
      expect(result.batches).toBe(2);
      expect(mockConnection.query).toHaveBeenCalledTimes(3); 
    });

    test('should handle empty data insertion', async () => {
      const service = new DataService();
      await service.initialize();

      const mockConnection = { query: jest.fn() };

      const result = await service.insertTableData(mockConnection, 'users', []);

      expect(result).toEqual({
        tableName: 'users',
        insertedRows: 0,
        totalRows: 0,
        success: true,
        batches: 0
      });
      expect(mockConnection.query).not.toHaveBeenCalled();
    });

    test('should clear table before insertion when requested', async () => {
      const service = new DataService();
      await service.initialize();

      const mockConnection = {
        query: jest.fn()
          .mockResolvedValueOnce({ rowCount: 10 })
          .mockResolvedValueOnce({
            rows: [{ column_name: 'id' }, { column_name: 'name' }]
          })
          .mockResolvedValueOnce({ rowCount: 2 })
      };

      const testData = [{ id: 1, name: 'User 1' }, { id: 2, name: 'User 2' }];

      const result = await service.insertTableData(mockConnection, 'users', testData, {
        clearFirst: true
      });

      expect(result.success).toBe(true);
      expect(mockConnection.query).toHaveBeenCalledWith('DELETE FROM "users";');
    });

    test('should handle insertion errors gracefully', async () => {
      const service = new DataService();
      await service.initialize();

      const mockConnection = {
        query: jest.fn()
          .mockResolvedValueOnce({
            rows: [{ column_name: 'id' }, { column_name: 'name' }]
          })
          .mockRejectedValueOnce(new Error('Constraint violation'))
      };

      const testData = [{ id: 1, name: 'User 1' }];

      const result = await service.insertTableData(mockConnection, 'users', testData);

      expect(result.success).toBe(false);
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0].error).toContain('Constraint violation');
    });

    test('should insert batch with conflict resolution', async () => {
      const service = new DataService();
      await service.initialize();

      const mockConnection = {
        query: jest.fn().mockResolvedValue({ rowCount: 2 })
      };

      const batch = [{ id: 1, name: 'User 1' }, { id: 2, name: 'User 2' }];
      const columns = ['id', 'name'];

      const result = await service.insertBatch(mockConnection, 'users', batch, columns, 'skip');

      expect(result.insertedRows).toBe(2);
      expect(mockConnection.query).toHaveBeenCalledWith(
        expect.stringContaining('ON CONFLICT DO NOTHING'),
        [1, 'User 1', 2, 'User 2']
      );
    });

    test('should insert batch with update on conflict', async () => {
      const service = new DataService();
      await service.initialize();

      const mockConnection = {
        query: jest.fn().mockResolvedValue({ rowCount: 2 })
      };

      const batch = [{ id: 1, name: 'User 1' }];
      const columns = ['id', 'name'];

      const result = await service.insertBatch(mockConnection, 'users', batch, columns, 'update');

      expect(result.insertedRows).toBe(2);
      expect(mockConnection.query).toHaveBeenCalledWith(
        expect.stringContaining('ON CONFLICT DO UPDATE SET'),
        [1, 'User 1']
      );
    });

    test('should handle batch with no valid columns', async () => {
      const service = new DataService();
      await service.initialize();

      const mockConnection = { query: jest.fn() };
      const batch = [{ invalid_col: 'value' }];
      const columns = ['id', 'name'];

      await expect(service.insertBatch(mockConnection, 'users', batch, columns, 'error'))
        .rejects.toThrow('No valid columns found for table users');
    });

    test('should synchronize table data', async () => {
      const service = new DataService();
      await service.initialize();

      const sourceConnection = {
        query: jest.fn()
          .mockResolvedValueOnce({ rows: [{ count: '2' }] })
          .mockResolvedValueOnce({
            rows: [{ id: 1, name: 'User 1' }, { id: 2, name: 'User 2' }],
            fields: [{ name: 'id' }, { name: 'name' }]
          })
      };

      const targetConnection = {
        query: jest.fn()
          .mockResolvedValueOnce({ rowCount: 0 })
          .mockResolvedValueOnce({
            rows: [{ column_name: 'id' }, { column_name: 'name' }]
          })
          .mockResolvedValueOnce({ rowCount: 2 })
      };

      const result = await service.syncTableData(sourceConnection, targetConnection, 'users');

      expect(result.success).toBe(true);
      expect(result.sourceRows).toBe(2);
      expect(result.insertedRows).toBe(2);
      expect(result.tableName).toBe('users');
    });

    test('should synchronize all data from multiple tables', async () => {
      const service = new DataService();
      await service.initialize();

      const sourceConnection = {
        query: jest.fn()
          .mockResolvedValueOnce({
            rows: [{ table_name: 'users' }, { table_name: 'orders' }]
          })
          .mockResolvedValueOnce({
            rows: [{ dependent_table: 'orders', referenced_table: 'users' }]
          })
          .mockResolvedValueOnce({ rows: [{ count: '1' }] })
          .mockResolvedValueOnce({
            rows: [{ id: 1, name: 'User 1' }],
            fields: [{ name: 'id' }, { name: 'name' }]
          })
          .mockResolvedValueOnce({ rows: [{ count: '1' }] })
          .mockResolvedValueOnce({
            rows: [{ id: 1, user_id: 1, total: 100 }],
            fields: [{ name: 'id' }, { name: 'user_id' }, { name: 'total' }]
          })
      };

      const targetConnection = {
        query: jest.fn()
          .mockResolvedValueOnce({ rowCount: 0 })
          .mockResolvedValueOnce({
            rows: [{ column_name: 'id' }, { column_name: 'name' }]
          })
          .mockResolvedValueOnce({ rowCount: 1 })
          .mockResolvedValueOnce({ rowCount: 0 })
          .mockResolvedValueOnce({
            rows: [{ column_name: 'id' }, { column_name: 'user_id' }, { column_name: 'total' }]
          })
          .mockResolvedValueOnce({ rowCount: 1 })
      };

      const onTableStart = jest.fn();
      const onTableComplete = jest.fn();

      const result = await service.syncAllData(sourceConnection, targetConnection, {
        onTableStart,
        onTableComplete
      });

      expect(result.success).toBe(true);
      expect(result.totalTables).toBe(2);
      expect(result.totalSourceRows).toBe(2);
      expect(result.totalInsertedRows).toBe(2);
      expect(result.tableOrder).toEqual(['users', 'orders']); 
      expect(onTableStart).toHaveBeenCalledTimes(2);
      expect(onTableComplete).toHaveBeenCalledTimes(2);
    });

    test('should handle sync errors and continue with other tables', async () => {
      const service = new DataService();
      await service.initialize();

      const sourceConnection = {
        query: jest.fn()
          .mockResolvedValueOnce({
            rows: [{ table_name: 'users' }, { table_name: 'bad_table' }]
          })
          .mockResolvedValueOnce({ rows: [] })
          .mockResolvedValueOnce({ rows: [{ count: '1' }] })
          .mockResolvedValueOnce({
            rows: [{ id: 1, name: 'User 1' }],
            fields: [{ name: 'id' }, { name: 'name' }]
          })
          .mockRejectedValueOnce(new Error('Table does not exist'))
      };

      const targetConnection = {
        query: jest.fn()
          .mockResolvedValueOnce({ rowCount: 0 })
          .mockResolvedValueOnce({
            rows: [{ column_name: 'id' }, { column_name: 'name' }]
          })
          .mockResolvedValueOnce({ rowCount: 1 })
      };

      const result = await service.syncAllData(sourceConnection, targetConnection);

      expect(result.success).toBe(false);
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0].tableName).toBe('bad_table');
      expect(result.tables).toHaveLength(1); 
      expect(result.tables[0].success).toBe(true);
    });

    test('should throw error when not initialized for insertion operations', async () => {
      const service = new DataService();
      const mockConnection = { query: jest.fn() };

      await expect(service.clearTableData(mockConnection, 'users')).rejects.toThrow('Data service not initialized');
      await expect(service.insertTableData(mockConnection, 'users', [])).rejects.toThrow('Data service not initialized');
      await expect(service.syncTableData(mockConnection, mockConnection, 'users')).rejects.toThrow('Data service not initialized');
      await expect(service.syncAllData(mockConnection, mockConnection)).rejects.toThrow('Data service not initialized');
    });
  });
});
