import { jest } from '@jest/globals';
import { SyncOrchestratorService, syncOrchestratorService } from '../../src/services/syncOrchestrator.js';

// Mock all the services
jest.unstable_mockModule('../../src/services/metabase.js', () => ({
  metabaseService: {
    authenticate: jest.fn(),
    getTables: jest.fn(),
    getTableRowCount: jest.fn(),
    extractAllTableData: jest.fn(),
    logout: jest.fn()
  }
}));

jest.unstable_mockModule('../../src/services/connection.js', () => ({
  connectionService: {
    initialize: jest.fn(),
    connectLocal: jest.fn(),
    closeConnections: jest.fn()
  }
}));

jest.unstable_mockModule('../../src/services/data.js', () => ({
  dataService: {
    initialize: jest.fn(),
    getTableDependencies: jest.fn(),
    sortTablesByDependencies: jest.fn(),
    insertTableData: jest.fn()
  }
}));

jest.unstable_mockModule('../../src/services/schemaDiscovery.js', () => ({
  schemaDiscoveryService: {
    discoverEnumValues: jest.fn()
  }
}));

jest.unstable_mockModule('../../src/services/dataTransformation.js', () => ({
  dataTransformationService: {
    transformTableData: jest.fn(),
    getTransformationStats: jest.fn()
  }
}));

const { metabaseService } = await import('../../src/services/metabase.js');
const { connectionService } = await import('../../src/services/connection.js');
const { dataService } = await import('../../src/services/data.js');
const { schemaDiscoveryService } = await import('../../src/services/schemaDiscovery.js');
const { dataTransformationService } = await import('../../src/services/dataTransformation.js');

describe('SyncOrchestratorService', () => {
  let service;
  let mockConnection;

  beforeEach(() => {
    service = new SyncOrchestratorService();
    mockConnection = { query: jest.fn() };
    jest.clearAllMocks();

    // Setup default mocks
    metabaseService.authenticate.mockResolvedValue({ success: true });
    connectionService.initialize.mockResolvedValue();
    dataService.initialize.mockResolvedValue();
    connectionService.connectLocal.mockResolvedValue(mockConnection);
    metabaseService.getTables.mockResolvedValue({
      success: true,
      tables: [
        { id: 1, name: 'users' },
        { id: 2, name: 'orders' }
      ]
    });
    dataService.getTableDependencies.mockResolvedValue({});
    dataService.sortTablesByDependencies.mockReturnValue(['users', 'orders']);
    schemaDiscoveryService.discoverEnumValues.mockResolvedValue({});
    metabaseService.getTableRowCount.mockResolvedValue({ success: true, count: 10 });
    metabaseService.extractAllTableData.mockResolvedValue({
      success: true,
      data: [{ id: 1, name: 'test' }]
    });
    dataTransformationService.transformTableData.mockResolvedValue([{ id: 1, name: 'test' }]);
    dataService.insertTableData.mockResolvedValue({
      success: true,
      insertedRows: 1
    });
    dataTransformationService.getTransformationStats.mockReturnValue({});
  });

  describe('executeSync', () => {
    test('should execute complete sync successfully', async () => {
      const credentials = { username: 'test', password: 'test' };

      const result = await service.executeSync(credentials);

      expect(result.success).toBe(true);
      expect(result.totalTables).toBe(2);
      expect(result.successfulTables).toBe(2);
      expect(metabaseService.authenticate).toHaveBeenCalledWith('test', 'test');
      expect(connectionService.initialize).toHaveBeenCalled();
      expect(dataService.initialize).toHaveBeenCalled();
    });

    test('should handle authentication failure', async () => {
      metabaseService.authenticate.mockResolvedValue({
        success: false,
        error: 'Invalid credentials'
      });

      const credentials = { username: 'test', password: 'wrong' };

      await expect(service.executeSync(credentials)).rejects.toThrow(
        'Failed to authenticate with Metabase: Invalid credentials'
      );
    });

    test('should handle table discovery failure', async () => {
      metabaseService.getTables.mockResolvedValue({
        success: false,
        error: 'API error'
      });

      const credentials = { username: 'test', password: 'test' };

      await expect(service.executeSync(credentials)).rejects.toThrow(
        'Failed to retrieve tables: API error'
      );
    });
  });

  describe('syncSingleTable', () => {
    test('should sync single table successfully', async () => {
      const table = { id: 1, name: 'users' };
      const enumMap = { status_enum: ['ACTIVE', 'INACTIVE'] };

      await service.syncSingleTable(mockConnection, table, enumMap);

      expect(metabaseService.extractAllTableData).toHaveBeenCalledWith(1, 'users');
      expect(dataTransformationService.transformTableData).toHaveBeenCalled();
      expect(dataService.insertTableData).toHaveBeenCalled();
    });

    test('should handle data extraction failure', async () => {
      metabaseService.extractAllTableData.mockResolvedValue({
        success: false,
        error: 'Extraction failed'
      });

      const table = { id: 1, name: 'users' };

      await expect(service.syncSingleTable(mockConnection, table, {})).rejects.toThrow(
        'Data extraction failed: Extraction failed'
      );
    });

    test('should handle data insertion failure', async () => {
      dataService.insertTableData.mockResolvedValue({
        success: false,
        errors: [{ error: 'Constraint violation' }]
      });

      const table = { id: 1, name: 'users' };

      await expect(service.syncSingleTable(mockConnection, table, {})).rejects.toThrow(
        'Data insertion failed: Constraint violation'
      );
    });

    test('should handle row count mismatch', async () => {
      metabaseService.extractAllTableData.mockResolvedValue({
        success: true,
        data: [{ id: 1 }, { id: 2 }]
      });

      dataService.insertTableData.mockResolvedValue({
        success: true,
        insertedRows: 1
      });

      const table = { id: 1, name: 'users' };

      await expect(service.syncSingleTable(mockConnection, table, {})).rejects.toThrow(
        'Row count mismatch: expected 2, inserted 1'
      );
    });
  });

  describe('handleSyncFailures', () => {
    test('should perform rollback on sync failures', async () => {
      service.syncStats.failedTables = [
        { name: 'users', error: 'Test error', details: 'Test details' }
      ];

      const tables = [{ name: 'users' }];
      const dependencies = {};

      await expect(service.handleSyncFailures(tables, dependencies)).rejects.toThrow(
        'Database synchronization FAILED - no changes applied'
      );

      expect(mockConnection.query).toHaveBeenCalledWith('DELETE FROM "users"');
    });
  });

  describe('configuration', () => {
    test('should configure sync options', () => {
      const config = { batchSize: 500, onConflict: 'skip' };

      service.configure(config);

      expect(service.syncConfig.batchSize).toBe(500);
      expect(service.syncConfig.onConflict).toBe('skip');
    });

    test('should get sync statistics', () => {
      service.syncStats.totalTables = 5;
      service.syncStats.successfulTables = 3;

      const stats = service.getSyncStats();

      expect(stats.totalTables).toBe(5);
      expect(stats.successfulTables).toBe(3);
    });
  });

  describe('cleanup', () => {
    test('should cleanup resources', async () => {
      await service.cleanup();

      expect(connectionService.closeConnections).toHaveBeenCalled();
      expect(metabaseService.logout).toHaveBeenCalled();
    });

    test('should handle cleanup errors gracefully', async () => {
      connectionService.closeConnections.mockRejectedValue(new Error('Cleanup error'));

      await expect(service.cleanup()).resolves.not.toThrow();
    });
  });

  describe('singleton instance', () => {
    test('should be an instance of SyncOrchestratorService', () => {
      expect(syncOrchestratorService).toBeInstanceOf(SyncOrchestratorService);
    });
  });
});