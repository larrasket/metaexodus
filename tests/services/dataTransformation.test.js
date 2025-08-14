import { jest } from '@jest/globals';

// Mock the schemaDiscoveryService BEFORE importing anything else
const mockSchemaDiscoveryService = {
  discoverTableSchema: jest.fn()
};

jest.unstable_mockModule('../../src/services/schemaDiscovery.js', () => ({
  schemaDiscoveryService: mockSchemaDiscoveryService
}));

// Now import the service
const { DataTransformationService, dataTransformationService } = await import(
  '../../src/services/dataTransformation.js'
);

describe('DataTransformationService', () => {
  let service;
  let mockConnection;

  beforeEach(() => {
    service = new DataTransformationService();
    mockConnection = {
      query: jest.fn().mockResolvedValue({ rows: [] })
    };
    jest.clearAllMocks();
    service.resetStats();
  });

  describe('transformTableData', () => {
    test('should transform enum values correctly', async () => {
      const mockSchema = [
        {
          column_name: 'status',
          data_type: 'USER-DEFINED',
          udt_name: 'status_enum',
          is_nullable: 'YES'
        }
      ];

      mockSchemaDiscoveryService.discoverTableSchema.mockResolvedValue(
        mockSchema
      );

      const data = [
        { id: 1, status: 'ACTIVITY' },
        { id: 2, status: 'USER' }
      ];

      const enumMap = {
        status_enum: ['INDIVIDUAL', 'GROUP', 'ALL']
      };

      const result = await service.transformTableData(
        mockConnection,
        'test_table',
        data,
        enumMap
      );

      expect(result).toHaveLength(2);
      expect(result[0].status).toBe('INDIVIDUAL'); // ACTIVITY -> INDIVIDUAL (common mapping)
      expect(result[1].status).toBe('INDIVIDUAL'); // USER -> INDIVIDUAL (common mapping)
    });

    test('should handle empty data', async () => {
      const result = await service.transformTableData(
        mockConnection,
        'test_table',
        [],
        {}
      );

      expect(result).toEqual([]);
    });

    test('should handle tables with no enum columns', async () => {
      mockSchemaDiscoveryService.discoverTableSchema.mockResolvedValue([
        {
          column_name: 'id',
          data_type: 'integer',
          udt_name: 'int4',
          is_nullable: 'NO'
        }
      ]);

      const data = [{ id: 1, name: 'test' }];
      const result = await service.transformTableData(
        mockConnection,
        'test_table',
        data,
        {}
      );

      expect(result).toEqual(data);
    });
  });

  describe('transformEnumValue', () => {
    test('should return exact match', () => {
      const result = service.transformEnumValue(
        'ACTIVE',
        ['ACTIVE', 'INACTIVE'],
        'test',
        'status'
      );

      expect(result).toBe('ACTIVE');
    });

    test('should perform case insensitive match', () => {
      const result = service.transformEnumValue(
        'active',
        ['ACTIVE', 'INACTIVE'],
        'test',
        'status'
      );

      expect(result).toBe('ACTIVE');
    });

    test('should perform partial match', () => {
      const result = service.transformEnumValue(
        'ACT',
        ['ACTIVE', 'INACTIVE'],
        'test',
        'status'
      );

      expect(result).toBe('ACTIVE');
    });

    test('should use common mappings', () => {
      const result = service.transformEnumValue(
        'activity',
        ['INDIVIDUAL', 'GROUP'],
        'test',
        'type'
      );

      expect(result).toBe('INDIVIDUAL');
    });

    test('should default to first valid value', () => {
      const result = service.transformEnumValue(
        'unknown',
        ['FIRST', 'SECOND'],
        'test',
        'status'
      );

      expect(result).toBe('FIRST');
    });

    test('should return null for empty valid values', () => {
      const result = service.transformEnumValue(
        'unknown',
        [],
        'test',
        'status'
      );

      expect(result).toBeNull();
    });
  });

  describe('getCommonEnumMappings', () => {
    test('should return common enum mappings', () => {
      const mappings = service.getCommonEnumMappings();

      expect(mappings.activity).toBe('INDIVIDUAL');
      expect(mappings.user).toBe('INDIVIDUAL');
      expect(mappings.event_details).toBe('EVENT');
      expect(mappings.active).toBe('ACTIVE');
      expect(mappings.true).toBe('TRUE');
    });
  });

  describe('transformDataType', () => {
    test('should transform integer values', () => {
      expect(service.transformDataType('123', 'integer')).toBe(123);
      expect(service.transformDataType('invalid', 'integer')).toBeNull();
    });

    test('should transform numeric values', () => {
      expect(service.transformDataType('123.45', 'numeric')).toBe(123.45);
      expect(service.transformDataType('invalid', 'numeric')).toBeNull();
    });

    test('should transform boolean values', () => {
      expect(service.transformDataType('true', 'boolean')).toBe(true);
      expect(service.transformDataType('false', 'boolean')).toBe(false);
      expect(service.transformDataType('1', 'boolean')).toBe(true);
      expect(service.transformDataType('0', 'boolean')).toBe(false);
      expect(service.transformDataType('yes', 'boolean')).toBe(true);
    });

    test('should transform date values', () => {
      const dateStr = '2023-01-01';
      const result = service.transformDataType(dateStr, 'date');

      expect(result).toBeInstanceOf(Date);
      expect(service.transformDataType('invalid-date', 'date')).toBeNull();
    });

    test('should transform JSON values', () => {
      const obj = { key: 'value' };
      const result = service.transformDataType(obj, 'json');

      expect(result).toBe('{"key":"value"}');
    });

    test('should handle null values', () => {
      expect(service.transformDataType(null, 'integer')).toBeNull();
      expect(service.transformDataType(undefined, 'integer')).toBeNull();
    });

    test('should default to string conversion', () => {
      expect(service.transformDataType(123, 'text')).toBe('123');
      expect(service.transformDataType(true, 'varchar')).toBe('true');
    });
  });

  describe('validateTransformedData', () => {
    test('should validate data against schema constraints', () => {
      const data = [
        { id: 1, name: 'test' },
        { id: 2, name: null }
      ];

      const schema = [
        { column_name: 'id', is_nullable: 'NO', column_default: null },
        { column_name: 'name', is_nullable: 'NO', column_default: null }
      ];

      const result = service.validateTransformedData(data, schema);

      expect(result.valid).toBe(false);
      expect(result.issues).toHaveLength(1);
      expect(result.issues[0].type).toBe('null_constraint_violation');
      expect(result.issues[0].column).toBe('name');
    });

    test('should pass validation for valid data', () => {
      const data = [
        { id: 1, name: 'test1' },
        { id: 2, name: 'test2' }
      ];

      const schema = [
        { column_name: 'id', is_nullable: 'NO', column_default: null },
        { column_name: 'name', is_nullable: 'NO', column_default: null }
      ];

      const result = service.validateTransformedData(data, schema);

      expect(result.valid).toBe(true);
      expect(result.issues).toHaveLength(0);
    });
  });

  describe('statistics', () => {
    test('should track transformation statistics', () => {
      service.transformationStats.totalTransformations = 5;
      service.transformationStats.enumTransformations = 3;

      const stats = service.getTransformationStats();

      expect(stats.totalTransformations).toBe(5);
      expect(stats.enumTransformations).toBe(3);
    });

    test('should reset statistics', () => {
      service.transformationStats.totalTransformations = 5;
      service.resetStats();

      const stats = service.getTransformationStats();

      expect(stats.totalTransformations).toBe(0);
      expect(stats.enumTransformations).toBe(0);
    });
  });

  describe('singleton instance', () => {
    test('should be an instance of DataTransformationService', () => {
      expect(dataTransformationService).toBeInstanceOf(
        DataTransformationService
      );
    });
  });
});
