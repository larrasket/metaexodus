import { jest } from '@jest/globals';
import { DataTransformationService, dataTransformationService } from '../../src/services/dataTransformation.js';

// Mock the schema discovery service
jest.unstable_mockModule('../../src/services/schemaDiscovery.js', () => ({
  schemaDiscoveryService: {
    getEnumColumns: jest.fn()
  }
}));

const { schemaDiscoveryService } = await import('../../src/services/schemaDiscovery.js');

describe('DataTransformationService', () => {
  let service;
  let mockConnection;

  beforeEach(() => {
    service = new DataTransformationService();
    mockConnection = { query: jest.fn() };
    jest.clearAllMocks();
    service.resetStats();
  });

  describe('transformTableData', () => {
    test('should transform enum values correctly', async () => {
      const enumColumns = [
        {
          column_name: 'status',
          udt_name: 'status_enum'
        }
      ];
      
      schemaDiscoveryService.getEnumColumns.mockResolvedValue(enumColumns);

      const data = [
        { id: 1, status: 'ACTIVE', name: 'Test' },
        { id: 2, status: 'inactive', name: 'Test2' }
      ];

      const enumMap = {
        status_enum: ['ACTIVE', 'INACTIVE', 'PENDING']
      };

      const result = await service.transformTableData(mockConnection, 'users', data, enumMap);

      expect(result).toHaveLength(2);
      expect(result[0].status).toBe('ACTIVE'); // Already valid
      expect(result[1].status).toBe('INACTIVE'); // Case-insensitive match
    });

    test('should handle invalid enum values with defaults', async () => {
      const enumColumns = [
        {
          column_name: 'type',
          udt_name: 'type_enum'
        }
      ];
      
      schemaDiscoveryService.getEnumColumns.mockResolvedValue(enumColumns);

      const data = [
        { id: 1, type: 'INVALID_TYPE' }
      ];

      const enumMap = {
        type_enum: ['USER', 'ADMIN']
      };

      const result = await service.transformTableData(mockConnection, 'users', data, enumMap);

      expect(result[0].type).toBe('USER'); // Default to first valid value
    });

    test('should handle empty data', async () => {
      const result = await service.transformTableData(mockConnection, 'users', [], {});

      expect(result).toEqual([]);
    });

    test('should handle tables with no enum columns', async () => {
      schemaDiscoveryService.getEnumColumns.mockResolvedValue([]);

      const data = [{ id: 1, name: 'Test' }];
      const result = await service.transformTableData(mockConnection, 'users', data, {});

      expect(result).toEqual(data);
    });

    test('should handle null and undefined values', async () => {
      const enumColumns = [
        {
          column_name: 'status',
          udt_name: 'status_enum'
        }
      ];
      
      schemaDiscoveryService.getEnumColumns.mockResolvedValue(enumColumns);

      const data = [
        { id: 1, status: null },
        { id: 2, status: undefined }
      ];

      const enumMap = {
        status_enum: ['ACTIVE', 'INACTIVE']
      };

      const result = await service.transformTableData(mockConnection, 'users', data, enumMap);

      expect(result[0].status).toBeNull();
      expect(result[1].status).toBeUndefined();
    });
  });

  describe('transformEnumValue', () => {
    test('should find case-insensitive matches', () => {
      const validValues = ['ACTIVE', 'INACTIVE'];
      const result = service.transformEnumValue('active', validValues, 'users', 'status');

      expect(result).toBe('ACTIVE');
    });

    test('should find partial matches', () => {
      const validValues = ['INDIVIDUAL', 'GROUP'];
      const result = service.transformEnumValue('ACTIVITY', validValues, 'users', 'type');

      expect(result).toBe('INDIVIDUAL'); // Common abbreviation mapping
    });

    test('should default to first valid value', () => {
      const validValues = ['USER', 'ADMIN'];
      const result = service.transformEnumValue('UNKNOWN', validValues, 'users', 'role');

      expect(result).toBe('USER');
    });

    test('should return null when no valid values', () => {
      const result = service.transformEnumValue('UNKNOWN', [], 'users', 'role');

      expect(result).toBeNull();
    });
  });

  describe('findPartialMatch', () => {
    test('should find contained matches', () => {
      const validValues = ['EVENT_DETAILS', 'NEWS_DETAILS'];
      const result = service.findPartialMatch('EVENT', validValues);

      expect(result).toBe('EVENT_DETAILS');
    });

    test('should find reverse contained matches', () => {
      const validValues = ['EVENT'];
      const result = service.findPartialMatch('EVENT_DETAILS', validValues);

      expect(result).toBe('EVENT');
    });

    test('should return null when no matches', () => {
      const validValues = ['USER', 'ADMIN'];
      const result = service.findPartialMatch('UNKNOWN', validValues);

      expect(result).toBeNull();
    });
  });

  describe('isCommonAbbreviation', () => {
    test('should recognize common abbreviations', () => {
      expect(service.isCommonAbbreviation('activity', 'act')).toBe(true);
      expect(service.isCommonAbbreviation('individual', 'user')).toBe(true);
      expect(service.isCommonAbbreviation('event', 'event_details')).toBe(true);
    });

    test('should return false for non-abbreviations', () => {
      expect(service.isCommonAbbreviation('random', 'other')).toBe(false);
    });
  });

  describe('validateTransformedData', () => {
    test('should validate data successfully', async () => {
      mockConnection.query.mockResolvedValue({
        rows: [
          {
            column_name: 'id',
            is_nullable: 'NO',
            column_default: 'nextval(...)'
          },
          {
            column_name: 'name',
            is_nullable: 'YES',
            column_default: null
          }
        ]
      });

      const data = [
        { id: 1, name: 'Test' },
        { id: 2, name: null }
      ];

      const result = await service.validateTransformedData(mockConnection, 'users', data);

      expect(result.valid).toBe(true);
      expect(result.issues).toHaveLength(0);
    });

    test('should detect null constraint violations', async () => {
      mockConnection.query.mockResolvedValue({
        rows: [
          {
            column_name: 'name',
            is_nullable: 'NO',
            column_default: null
          }
        ]
      });

      const data = [
        { id: 1, name: null }
      ];

      const result = await service.validateTransformedData(mockConnection, 'users', data);

      expect(result.valid).toBe(false);
      expect(result.issues).toHaveLength(1);
      expect(result.issues[0].type).toBe('null_constraint_violation');
    });

    test('should handle empty data validation', async () => {
      const result = await service.validateTransformedData(mockConnection, 'users', []);

      expect(result.valid).toBe(true);
      expect(result.issues).toHaveLength(0);
    });
  });

  describe('statistics', () => {
    test('should track transformation statistics', async () => {
      const enumColumns = [
        {
          column_name: 'status',
          udt_name: 'status_enum'
        }
      ];
      
      schemaDiscoveryService.getEnumColumns.mockResolvedValue(enumColumns);

      const data = [
        { id: 1, status: 'invalid' },
        { id: 2, status: 'ACTIVE' }
      ];

      const enumMap = {
        status_enum: ['ACTIVE', 'INACTIVE']
      };

      await service.transformTableData(mockConnection, 'users', data, enumMap);

      const stats = service.getTransformationStats();

      expect(stats.totalTransformations).toBe(2);
      expect(stats.enumTransformations).toBe(1);
    });

    test('should reset statistics', () => {
      service.transformationStats.totalTransformations = 10;
      service.resetStats();

      const stats = service.getTransformationStats();

      expect(stats.totalTransformations).toBe(0);
      expect(stats.enumTransformations).toBe(0);
      expect(stats.nullTransformations).toBe(0);
    });
  });

  describe('singleton instance', () => {
    test('should be an instance of DataTransformationService', () => {
      expect(dataTransformationService).toBeInstanceOf(DataTransformationService);
    });

    test('should maintain state across imports', () => {
      dataTransformationService.transformationStats.totalTransformations = 5;
      const stats = dataTransformationService.getTransformationStats();

      expect(stats.totalTransformations).toBe(5);
    });
  });
});