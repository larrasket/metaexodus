import { jest } from '@jest/globals';
import { SchemaDiscoveryService, schemaDiscoveryService } from '../../src/services/schemaDiscovery.js';

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
      expect(mockConnection.query).toHaveBeenCalledWith(expect.stringContaining('pg_type'));
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

    test('should cache enum values', async () => {
      mockConnection.query.mockResolvedValue({
        rows: [{ enum_name: 'test_enum', enum_value: 'TEST' }]
      });

      // First call
      await service.discoverEnumValues(mockConnection);
      // Second call should use cache
      await service.discoverEnumValues(mockConnection);

      expect(mockConnection.query).toHaveBeenCalledTimes(1);
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

    test('should cache table schema', async () => {
      mockConnection.query.mockResolvedValue({
        rows: [{ column_name: 'id', data_type: 'integer' }]
      });

      // First call
      await service.discoverTableSchema(mockConnection, 'users');
      // Second call should use cache
      await service.discoverTableSchema(mockConnection, 'users');

      expect(mockConnection.query).toHaveBeenCalledTimes(1);
    });

    test('should handle schema discovery errors', async () => {
      mockConnection.query.mockRejectedValue(new Error('Schema error'));

      const result = await service.discoverTableSchema(mockConnection, 'users');

      expect(result).toEqual([]);
    });
  });

  describe('getEnumColumns', () => {
    test('should return enum columns for table', async () => {
      mockConnection.query.mockResolvedValue({
        rows: [
          {
            column_name: 'id',
            data_type: 'integer',
            udt_name: 'int4',
            is_nullable: 'NO'
          },
          {
            column_name: 'status',
            data_type: 'USER-DEFINED',
            udt_name: 'status_enum',
            is_nullable: 'YES'
          }
        ]
      });

      const enumMap = { status_enum: ['ACTIVE', 'INACTIVE'] };
      const result = await service.getEnumColumns(mockConnection, 'users', enumMap);

      expect(result).toHaveLength(1);
      expect(result[0].column_name).toBe('status');
      expect(result[0].udt_name).toBe('status_enum');
    });

    test('should return empty array when no enum columns', async () => {
      mockConnection.query.mockResolvedValue({
        rows: [
          {
            column_name: 'id',
            data_type: 'integer',
            udt_name: 'int4',
            is_nullable: 'NO'
          }
        ]
      });

      const enumMap = {};
      const result = await service.getEnumColumns(mockConnection, 'users', enumMap);

      expect(result).toHaveLength(0);
    });
  });

  describe('cache management', () => {
    test('should clear specific table cache', async () => {
      mockConnection.query.mockResolvedValue({
        rows: [{ column_name: 'id', data_type: 'integer' }]
      });

      await service.discoverTableSchema(mockConnection, 'users');
      service.clearCache('users');
      await service.discoverTableSchema(mockConnection, 'users');

      expect(mockConnection.query).toHaveBeenCalledTimes(2);
    });

    test('should clear all cache', async () => {
      mockConnection.query.mockResolvedValue({ rows: [] });

      await service.discoverEnumValues(mockConnection);
      await service.discoverTableSchema(mockConnection, 'users');
      
      service.clearCache();
      
      await service.discoverEnumValues(mockConnection);
      await service.discoverTableSchema(mockConnection, 'users');

      expect(mockConnection.query).toHaveBeenCalledTimes(4);
    });

    test('should return cache statistics', async () => {
      mockConnection.query.mockResolvedValue({ rows: [] });

      await service.discoverEnumValues(mockConnection);
      await service.discoverTableSchema(mockConnection, 'users');
      await service.discoverTableSchema(mockConnection, 'orders');

      const stats = service.getCacheStats();

      expect(stats.enumCacheSize).toBe(1);
      expect(stats.schemaCacheSize).toBe(2);
      expect(stats.cachedTables).toContain('users');
      expect(stats.cachedTables).toContain('orders');
    });
  });

  describe('singleton instance', () => {
    test('should be an instance of SchemaDiscoveryService', () => {
      expect(schemaDiscoveryService).toBeInstanceOf(SchemaDiscoveryService);
    });

    test('should maintain state across imports', async () => {
      mockConnection.query.mockResolvedValue({ rows: [] });

      await schemaDiscoveryService.discoverEnumValues(mockConnection);
      const stats = schemaDiscoveryService.getCacheStats();

      expect(stats.enumCacheSize).toBe(1);
    });
  });
});