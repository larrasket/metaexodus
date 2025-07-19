import { jest } from '@jest/globals';

// Create mock objects first
const mockMetabaseService = {
  authenticate: jest.fn(),
  getTables: jest.fn(),
  getTableRowCount: jest.fn(),
  extractAllTableData: jest.fn(),
  logout: jest.fn()
};

const mockConnectionService = {
  initialize: jest.fn(),
  connectLocal: jest.fn(),
  closeConnections: jest.fn()
};

const mockDataService = {
  initialize: jest.fn(),
  getTableDependencies: jest.fn(),
  sortTablesByDependencies: jest.fn(),
  insertTableData: jest.fn()
};

const mockSchemaDiscoveryService = {
  discoverEnumValues: jest.fn()
};

const mockDataTransformationService = {
  transformTableData: jest.fn(),
  getTransformationStats: jest.fn()
};

// Mock all the services
jest.unstable_mockModule('../../src/services/metabase.js', () => ({
  metabaseService: mockMetabaseService
}));

jest.unstable_mockModule('../../src/services/connection.js', () => ({
  connectionService: mockConnectionService
}));

jest.unstable_mockModule('../../src/services/data.js', () => ({
  dataService: mockDataService
}));

jest.unstable_mockModule('../../src/services/schemaDiscovery.js', () => ({
  schemaDiscoveryService: mockSchemaDiscoveryService
}));

jest.unstable_mockModule('../../src/services/dataTransformation.js', () => ({
  dataTransformationService: mockDataTransformationService
}));

const { SyncOrchestratorService, syncOrchestratorService } = await import('../../src/services/syncOrchestrator.js');

describe('SyncOrchestratorService', () => {
  let service;
  let mockConnection;

  beforeEach(() => {
    service = new SyncOrchestratorService();
    mockConnection = { query: jest.fn() };
    jest.clearAllMocks();

    // Setup default mocks
    mockMetabaseService.authenticate.mockResolvedValue({ success: true });
    mockConnectionService.initialize.mockResolvedValue();
    mockDataService.initialize.mockResolvedValue();
    mockConnectionService.connectLocal.mockResolvedValue(mockConnection);
    mockMetabaseService.getTables.mockResolvedValue({
      success: true,
      tables: [
        { id: 1, name: 'users' },
        { id: 2, name: 'orders' }
      ]
    });
    mockDataService.getTableDependencies.mockResolvedValue({});
    mockDataService.sortTablesByDependencies.mockReturnValue(['users', 'orders']);
    mockSchemaDiscoveryService.discoverEnumValues.mockResolvedValue({});
    mockMetabaseService.getTableRowCount.mockResolvedValue({ success: true, count: 10 });
    mockMetabaseService.extractAllTableData.mockResolvedValue({
      success: true,
      data: [{ id: 1, name: 'test' }]
    });
    mockDataTransformationService.transformTableData.mockResolvedValue([{ id: 1, name: 'test' }]);
    mockDataService.insertTableData.mockResolvedValue({
      success: true,
      insertedRows: 1
    });
    mockDataTransformationService.getTransformationStats.mockReturnValue({});
  });

  describe('executeSync', () => {
    test('should execute complete sync successfully', async () => {
      const credentials = { username: 'test', password: 'test' };

      const result = await service.executeSync(credentials);

      expect(result.success).toBe(true);
      expect(result.totalTables).toBe(2);
      expect(result.successfulTables).toBe(2);
      expect(mockMetabaseService.authenticate).toHaveBeenCalledWith('test', 'test');
      expect(mockConnectionService.initialize).toHaveBeenCalled();
      expect(mockDataService.initialize).toHaveBeenCalled();
    });

    test('should handle authentication failure', async () => {
      mockMetabaseService.authenticate.mockResolvedValue({
        success: false,
        error: 'Invalid credentials'
      });

      const credentials = { username: 'test', password: 'wrong' };

      await expect(service.executeSync(credentials)).rejects.toThrow(
        'Failed to authenticate with Metabase: Invalid credentials'
      );
    });

    test('should handle table discovery failure', async () => {
      mockMetabaseService.getTables.mockResolvedValue({
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

      expect(mockMetabaseService.extractAllTableData).toHaveBeenCalledWith(1, 'users');
      expect(mockDataTransformationService.transformTableData).toHaveBeenCalled();
      expect(mockDataService.insertTableData).toHaveBeenCalled();
    });

    test('should handle data extraction failure', async () => {
      mockMetabaseService.extractAllTableData.mockResolvedValue({
        success: false,
        error: 'Extraction failed'
      });

      const table = { id: 1, name: 'users' };

      await expect(service.syncSingleTable(mockConnection, table, {})).rejects.toThrow(
        'Data extraction failed: Extraction failed'
      );
    });

    test('should handle data insertion failure', async () => {
      mockDataService.insertTableData.mockResolvedValue({
        success: false,
        errors: [{ error: 'Constraint violation' }]
      });

      const table = { id: 1, name: 'users' };

      await expect(service.syncSingleTable(mockConnection, table, {})).rejects.toThrow(
        'Data insertion failed: Constraint violation'
      );
    });

    test('should handle row count mismatch', async () => {
      mockMetabaseService.extractAllTableData.mockResolvedValue({
        success: true,
        data: [{ id: 1 }, { id: 2 }]
      });

      mockDataService.insertTableData.mockResolvedValue({
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

      expect(mockConnectionService.closeConnections).toHaveBeenCalled();
      expect(mockMetabaseService.logout).toHaveBeenCalled();
    });

    test('should handle cleanup errors gracefully', async () => {
      mockConnectionService.closeConnections.mockRejectedValue(new Error('Cleanup error'));

      await expect(service.cleanup()).resolves.not.toThrow();
    });
  });

  describe('singleton instance', () => {
    test('should be an instance of SyncOrchestratorService', () => {
      expect(syncOrchestratorService).toBeInstanceOf(SyncOrchestratorService);
    });
  });
});