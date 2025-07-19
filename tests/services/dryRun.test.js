import { jest } from '@jest/globals';
import { SyncOrchestratorService } from '../../src/services/syncOrchestrator.js';

// Mock all dependencies
jest.mock('../../src/utils/logger.js', () => ({
  logger: {
    section: jest.fn(),
    subsection: jest.fn(),
    startSpinner: jest.fn(),
    stopSpinner: jest.fn(),
    createProgressBar: jest.fn(),
    updateProgress: jest.fn(),
    stopProgress: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    success: jest.fn(),
    table: jest.fn()
  }
}));

jest.mock('../../src/services/metabase.js', () => ({
  metabaseService: {
    authenticate: jest.fn(),
    getTables: jest.fn(),
    getTableRowCount: jest.fn(),
    extractTableData: jest.fn(),
    logout: jest.fn()
  }
}));

jest.mock('../../src/services/connection.js', () => ({
  connectionService: {
    initialize: jest.fn(),
    connectLocal: jest.fn(),
    closeConnections: jest.fn()
  }
}));

jest.mock('../../src/services/data.js', () => ({
  dataService: {
    initialize: jest.fn(),
    getTableDependencies: jest.fn(),
    sortTablesByDependencies: jest.fn()
  }
}));

jest.mock('../../src/services/schemaDiscovery.js', () => ({
  schemaDiscoveryService: {
    discoverEnumValues: jest.fn(),
    getTableSchemaInfo: jest.fn()
  }
}));

jest.mock('../../src/services/dataTransformation.js', () => ({
  dataTransformationService: {
    transformTableData: jest.fn()
  }
}));

import { logger } from '../../src/utils/logger.js';
import { metabaseService } from '../../src/services/metabase.js';
import { connectionService } from '../../src/services/connection.js';
import { dataService } from '../../src/services/data.js';
import { schemaDiscoveryService } from '../../src/services/schemaDiscovery.js';
import { dataTransformationService } from '../../src/services/dataTransformation.js';

describe('SyncOrchestratorService - Dry Run', () => {
  let service;
  let mockConnection;
  let mockCredentials;

  beforeEach(() => {
    service = new SyncOrchestratorService();
    mockConnection = {
      query: jest.fn()
    };
    mockCredentials = {
      username: 'test_user',
      password: 'test_password'
    };
    
    jest.clearAllMocks();

    // Setup default mock responses
    connectionService.connectLocal.mockResolvedValue(mockConnection);
    metabaseService.authenticate.mockResolvedValue({ success: true });
    metabaseService.getTables.mockResolvedValue({ 
      success: true, 
      tables: [
        { id: 1, name: 'users' },
        { id: 2, name: 'orders' }
      ] 
    });
    metabaseService.getTableRowCount.mockResolvedValue({ success: true, count: 100 });
    dataService.getTableDependencies.mockResolvedValue({ 'orders': ['users'] });
    dataService.sortTablesByDependencies.mockImplementation(tables => tables);
    schemaDiscoveryService.discoverEnumValues.mockResolvedValue({ 'status_enum': ['ACTIVE', 'INACTIVE'] });
    schemaDiscoveryService.getTableSchemaInfo.mockResolvedValue({ 
      enumColumns: [{ column_name: 'status', udt_name: 'status_enum' }] 
    });
  });

  describe('performDryRun', () => {
    test('should perform dry run analysis successfully', async () => {
      metabaseService.extractTableData.mockResolvedValue({
        success: true,
        data: [{ id: 1, name: 'Test User', status: 'ACTIVE' }]
      });

      dataTransformationService.transformTableData.mockResolvedValue({
        success: true,
        data: [{ id: 1, name: 'Test User', status: 'ACTIVE' }],
        issues: []
      });

      const result = await service.performDryRun(mockCredentials);

      expect(result.success).toBe(true);
      expect(result.summary).toBeDefined();
      expect(result.analysis).toBeDefined();
      expect(logger.section).toHaveBeenCalledWith('MetaExodus - Dry Run Analysis');
      expect(logger.success).toHaveBeenCalledWith('Dry run analysis completed successfully');
    });

    test('should handle authentication failure', async () => {
      metabaseService.authenticate.mockResolvedValue({ 
        success: false, 
        error: 'Invalid credentials' 
      });

      const result = await service.performDryRun(mockCredentials);

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    });

    test('should handle table discovery failure', async () => {
      metabaseService.getTables.mockResolvedValue({ 
        success: false, 
        error: 'API error' 
      });

      const result = await service.performDryRun(mockCredentials);

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    });

    test('should handle analysis failure', async () => {
      service.analyzePlannedChanges = jest.fn().mockResolvedValue({
        success: false,
        error: 'Analysis failed'
      });

      const result = await service.performDryRun(mockCredentials);

      expect(result.success).toBe(false);
      expect(result.error).toBe('Analysis failed');
    });
  });

  describe('analyzePlannedChanges', () => {
    test('should analyze tables with data correctly', async () => {
      const tables = [
        { id: 1, name: 'users' },
        { id: 2, name: 'orders' }
      ];
      const tableCounts = { users: 50, orders: 100 };
      const enumMap = { 'status_enum': ['ACTIVE', 'INACTIVE'] };

      metabaseService.extractTableData.mockResolvedValue({
        success: true,
        data: [{ id: 1, name: 'Test', status: 'ACTIVE' }]
      });

      dataTransformationService.transformTableData.mockResolvedValue({
        success: true,
        data: [{ id: 1, name: 'Test', status: 'ACTIVE' }],
        issues: []
      });

      const result = await service.analyzePlannedChanges(tables, tableCounts, enumMap);

      expect(result.success).toBe(true);
      expect(result.tablesWithData).toBe(2);
      expect(result.totalRowsToSync).toBe(150);
      expect(result.schemaChanges).toBe(2); // Both tables have enum columns
      expect(result.potentialIssues).toEqual([]);
    });

    test('should detect data transformation issues', async () => {
      const tables = [{ id: 1, name: 'users' }];
      const tableCounts = { users: 10 };
      const enumMap = {};

      metabaseService.extractTableData.mockResolvedValue({
        success: true,
        data: [{ id: 1, name: 'Test' }]
      });

      dataTransformationService.transformTableData.mockResolvedValue({
        success: false,
        issues: [{ message: 'Invalid enum value' }]
      });

      const result = await service.analyzePlannedChanges(tables, tableCounts, enumMap);

      expect(result.success).toBe(true);
      expect(result.potentialIssues.length).toBe(1);
      expect(result.potentialIssues[0].table).toBe('users');
      expect(result.potentialIssues[0].issue).toBe('Data transformation issues detected');
    });

    test('should detect transformation warnings', async () => {
      const tables = [{ id: 1, name: 'users' }];
      const tableCounts = { users: 10 };
      const enumMap = {};

      metabaseService.extractTableData.mockResolvedValue({
        success: true,
        data: [{ id: 1, name: 'Test' }]
      });

      dataTransformationService.transformTableData.mockResolvedValue({
        success: true,
        data: [{ id: 1, name: 'Test' }],
        issues: [
          { message: 'Enum value transformed' },
          { message: 'Date format adjusted' }
        ]
      });

      const result = await service.analyzePlannedChanges(tables, tableCounts, enumMap);

      expect(result.success).toBe(true);
      expect(result.dataTransformations).toBe(1);
      expect(result.potentialIssues.length).toBe(1);
      expect(result.potentialIssues[0].issue).toBe('2 data transformation(s) needed');
    });

    test('should handle tables with no data', async () => {
      const tables = [{ id: 1, name: 'empty_table' }];
      const tableCounts = { empty_table: 0 };
      const enumMap = {};

      const result = await service.analyzePlannedChanges(tables, tableCounts, enumMap);

      expect(result.success).toBe(true);
      expect(result.tablesWithData).toBe(0);
      expect(result.totalRowsToSync).toBe(0);
      expect(result.potentialIssues).toEqual([]);
    });

    test('should handle extraction errors', async () => {
      const tables = [{ id: 1, name: 'users' }];
      const tableCounts = { users: 10 };
      const enumMap = {};

      metabaseService.extractTableData.mockRejectedValue(new Error('Extraction failed'));

      const result = await service.analyzePlannedChanges(tables, tableCounts, enumMap);

      expect(result.success).toBe(true);
      expect(result.potentialIssues.length).toBe(1);
      expect(result.potentialIssues[0].issue).toBe('Analysis error');
      expect(result.potentialIssues[0].details).toBe('Extraction failed');
    });

    test('should handle analysis errors gracefully', async () => {
      connectionService.connectLocal.mockRejectedValue(new Error('Connection failed'));

      const result = await service.analyzePlannedChanges([], {}, {});

      expect(result.success).toBe(false);
      expect(result.error).toBe('Connection failed');
    });
  });

  describe('generateDryRunSummary', () => {
    test('should generate summary with correct calculations', () => {
      service.syncStats.startTime = Date.now() - 65000; // 1 minute 5 seconds ago

      const analysisResult = {
        tablesWithData: 5,
        totalRowsToSync: 10000,
        potentialIssues: [{ table: 'test', issue: 'test issue' }]
      };

      const summary = service.generateDryRunSummary(analysisResult, {});

      expect(summary.tablesWithData).toBe(5);
      expect(summary.totalRowsToSync).toBe(10000);
      expect(summary.potentialIssues).toBe(1);
      expect(summary.analysisDuration).toMatch(/1m \d+s/);
      expect(summary.estimatedDuration).toMatch(/~\d+m \d+s/);
    });

    test('should handle zero rows correctly', () => {
      service.syncStats.startTime = Date.now() - 1000; // 1 second ago

      const analysisResult = {
        tablesWithData: 0,
        totalRowsToSync: 0,
        potentialIssues: []
      };

      const summary = service.generateDryRunSummary(analysisResult, {});

      expect(summary.totalRowsToSync).toBe(0);
      expect(summary.estimatedDuration).toBe('~0m 1s'); // Minimum 1 second
    });
  });
});