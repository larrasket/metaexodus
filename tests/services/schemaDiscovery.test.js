import { jest } from '@jest/globals';
import {
  SchemaDiscoveryService,
  schemaDiscoveryService
} from '../../src/services/schemaDiscovery.js';

describe('SchemaDiscoveryService', () => {
  let service;
  let mockConnection;

  beforeEach(() => {
    service = new SchemaDiscoveryService();
    mockConnection = {
      query: jest.fn()
    };
    jest.clearAllMocks();
  });

  describe('discoverEnumValues', () => {
    test('should discover enum values correctly', async () => {
      mockConnection.query.mockResolvedValue({
        rows: [
          { enum_name: 'status_enum', enum_value: 'ACTIVE' },
          { enum_name: 'status_enum', enum_value: 'INACTIVE' },
          { enum_name: 'type_enum', enum_value: 'USER' },
          { enum_name: 'type_enum', enum_value: 'ADMIN' }
        ]
      });

      const result = await service.discoverEnumValues(mockConnection);

      expect(result).toEqual({
        status_enum: ['ACTIVE', 'INACTIVE'],
        type_enum: ['USER', 'ADMIN']
      });
      expect(mockConnection.query).toHaveBeenCalledWith(
        expect.stringContaining('pg_enum')
      );
    });

    test('should handle empty enum result', async () => {
      mockConnection.query.mockResolvedValue({ rows: [] });

      const result = await service.discoverEnumValues(mockConnection);

      expect(result).toEqual({});
    });

    test('should handle database errors gracefully', async () => {
      mockConnection.query.mockRejectedValue(new Error('Database error'));

      const result = await service.discoverEnumValues(mockConnection);

      expect(result).toEqual({});
    });
  });

  describe('discoverTableSchema', () => {
    test('should discover table schema correctly', async () => {
      mockConnection.query.mockResolvedValue({
        rows: [
          {
            column_name: 'id',
            data_type: 'integer',
            udt_name: 'int4',
            is_nullable: 'NO',
            column_default: 'nextval(...)',
            ordinal_position: 1
          },
          {
            column_name: 'status',
            data_type: 'USER-DEFINED',
            udt_name: 'status_enum',
            is_nullable: 'YES',
            column_default: null,
            ordinal_position: 2
          }
        ]
      });

      const result = await service.discoverTableSchema(mockConnection, 'users');

      expect(result).toHaveLength(2);
      expect(result[0].column_name).toBe('id');
      expect(result[1].udt_name).toBe('status_enum');
      expect(mockConnection.query).toHaveBeenCalledWith(
        expect.stringContaining('information_schema.columns'),
        ['users']
      );
    });

    test('should cache schema results', async () => {
      mockConnection.query.mockResolvedValue({ rows: [] });

      await service.discoverTableSchema(mockConnection, 'users');
      await service.discoverTableSchema(mockConnection, 'users');

      expect(mockConnection.query).toHaveBeenCalledTimes(1);
    });
  });

  describe('discoverForeignKeys', () => {
    test('should discover foreign keys correctly', async () => {
      mockConnection.query.mockResolvedValue({
        rows: [
          {
            constraint_name: 'fk_user_id',
            column_name: 'user_id',
            foreign_table_name: 'users',
            foreign_column_name: 'id'
          }
        ]
      });

      const result = await service.discoverForeignKeys(
        mockConnection,
        'orders'
      );

      expect(result).toHaveLength(1);
      expect(result[0].constraint_name).toBe('fk_user_id');
      expect(result[0].foreign_table_name).toBe('users');
    });
  });

  describe('discoverTables', () => {
    test('should discover all tables', async () => {
      mockConnection.query.mockResolvedValue({
        rows: [
          { table_name: 'users' },
          { table_name: 'orders' },
          { table_name: 'products' }
        ]
      });

      const result = await service.discoverTables(mockConnection);

      expect(result).toEqual(['users', 'orders', 'products']);
      expect(mockConnection.query).toHaveBeenCalledWith(
        expect.stringContaining('information_schema.tables')
      );
    });
  });

  describe('getTableSchemaInfo', () => {
    test('should get comprehensive table schema info', async () => {
      // Mock enum discovery
      service.schemaCache.set('enums', { status_enum: ['ACTIVE', 'INACTIVE'] });

      // Mock schema discovery
      mockConnection.query
        .mockResolvedValueOnce({
          rows: [
            {
              column_name: 'status',
              data_type: 'USER-DEFINED',
              udt_name: 'status_enum',
              is_nullable: 'YES',
              column_default: null,
              ordinal_position: 1
            }
          ]
        })
        .mockResolvedValueOnce({
          rows: [
            {
              constraint_name: 'fk_user_id',
              column_name: 'user_id',
              foreign_table_name: 'users',
              foreign_column_name: 'id'
            }
          ]
        });

      const result = await service.getTableSchemaInfo(mockConnection, 'orders');

      expect(result.tableName).toBe('orders');
      expect(result.columns).toHaveLength(1);
      expect(result.foreignKeys).toHaveLength(1);
      expect(result.enumColumns).toHaveLength(1);
    });
  });

  describe('cache management', () => {
    test('should clear specific cache key', () => {
      service.schemaCache.set('test_key', 'test_value');
      service.clearCache('test_key');

      expect(service.schemaCache.has('test_key')).toBe(false);
    });

    test('should clear entire cache', () => {
      service.schemaCache.set('key1', 'value1');
      service.schemaCache.set('key2', 'value2');

      service.clearCache();

      expect(service.schemaCache.size).toBe(0);
    });

    test('should return cache statistics', () => {
      service.schemaCache.set('key1', 'value1');
      service.schemaCache.set('key2', 'value2');

      const stats = service.getCacheStats();

      expect(stats.cacheSize).toBe(2);
      expect(stats.cachedKeys).toContain('key1');
      expect(stats.cachedKeys).toContain('key2');
    });
  });

  describe('singleton instance', () => {
    test('should be an instance of SchemaDiscoveryService', () => {
      expect(schemaDiscoveryService).toBeInstanceOf(SchemaDiscoveryService);
    });
  });
});
