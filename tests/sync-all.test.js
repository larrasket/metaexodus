import {jest} from '@jest/globals';

// Mock process.exit to prevent test termination
const mockExit = jest.spyOn(process, 'exit').mockImplementation(() => {
	throw new Error('process.exit() called');
});

// Mock console.log to capture output
const originalConsoleLog = console.log;
let consoleOutput = [];
console.log = jest.fn((...args) => {
	consoleOutput.push(args.join(' '));
});

// Mock the syncOrchestratorService
const mockSyncOrchestratorService = {
	performDryRun: jest.fn(),
	executeSync: jest.fn(),
	configure: jest.fn(),
};

jest.unstable_mockModule('../src/services/syncOrchestrator.js', () => ({
	syncOrchestratorService: mockSyncOrchestratorService,
}));

describe('sync-all.js', () => {
	beforeEach(() => {
		jest.clearAllMocks();
		consoleOutput = [];
		process.argv = ['node', 'sync-all.js'];
		delete process.env.DB_REMOTE_USERNAME;
		delete process.env.DB_REMOTE_PASSWORD;
	});

	afterAll(() => {
		console.log = originalConsoleLog;
		mockExit.mockRestore();
	});

	test('should show help when --help flag is used', async () => {
		process.argv = ['node', 'sync-all.js', '--help'];

		try {
			await import('../sync-all.js');
		} catch (error) {
			expect(error.message).toBe('process.exit() called');
		}

		expect(mockExit).toHaveBeenCalledWith(0);
		expect(consoleOutput.join('\n')).toContain('MetaExodus - Database Synchronization Tool');
		expect(consoleOutput.join('\n')).toContain('--ignore-errors, -i');
	});

	test('should show help when -h flag is used', async () => {
		process.argv = ['node', 'sync-all.js', '-h'];

		try {
			await import('../sync-all.js?' + Date.now()); // Add timestamp to avoid cache
		} catch (error) {
			expect(error.message).toBe('process.exit() called');
		}

		expect(mockExit).toHaveBeenCalledWith(0);
		expect(consoleOutput.join('\n')).toContain('MetaExodus - Database Synchronization Tool');
	});

	test('should configure continue on error mode when --ignore-errors is used', async () => {
		process.argv = ['node', 'sync-all.js', '--ignore-errors'];
		process.env.DB_REMOTE_USERNAME = 'test@example.com';
		process.env.DB_REMOTE_PASSWORD = 'password123';

		mockSyncOrchestratorService.executeSync.mockResolvedValue({success: true});

		try {
			await import('../sync-all.js?' + Date.now());
		} catch (error) {
			expect(error.message).toBe('process.exit() called');
		}

		expect(mockSyncOrchestratorService.configure).toHaveBeenCalledWith({
			enableRollback: false,
			continueOnError: true,
		});
		expect(mockSyncOrchestratorService.executeSync).toHaveBeenCalled();
		expect(mockExit).toHaveBeenCalledWith(0);
	});

	test('should configure continue on error mode when -i is used', async () => {
		process.argv = ['node', 'sync-all.js', '-i'];
		process.env.DB_REMOTE_USERNAME = 'test@example.com';
		process.env.DB_REMOTE_PASSWORD = 'password123';

		mockSyncOrchestratorService.executeSync.mockResolvedValue({success: true});

		try {
			await import('../sync-all.js?' + Date.now());
		} catch (error) {
			expect(error.message).toBe('process.exit() called');
		}

		expect(mockSyncOrchestratorService.configure).toHaveBeenCalledWith({
			enableRollback: false,
			continueOnError: true,
		});
		expect(mockSyncOrchestratorService.executeSync).toHaveBeenCalled();
		expect(mockExit).toHaveBeenCalledWith(0);
	});

	test('should perform dry run when --dry-run is used', async () => {
		process.argv = ['node', 'sync-all.js', '--dry-run'];
		process.env.DB_REMOTE_USERNAME = 'test@example.com';
		process.env.DB_REMOTE_PASSWORD = 'password123';

		mockSyncOrchestratorService.performDryRun.mockResolvedValue({success: true});

		try {
			await import('../sync-all.js?' + Date.now());
		} catch (error) {
			expect(error.message).toBe('process.exit() called');
		}

		expect(mockSyncOrchestratorService.performDryRun).toHaveBeenCalled();
		expect(mockSyncOrchestratorService.configure).not.toHaveBeenCalled();
		expect(mockExit).toHaveBeenCalledWith(0);
	});

	test('should execute normal sync without special configuration by default', async () => {
		process.argv = ['node', 'sync-all.js'];
		process.env.DB_REMOTE_USERNAME = 'test@example.com';
		process.env.DB_REMOTE_PASSWORD = 'password123';

		mockSyncOrchestratorService.executeSync.mockResolvedValue({success: true});

		try {
			await import('../sync-all.js?' + Date.now());
		} catch (error) {
			expect(error.message).toBe('process.exit() called');
		}

		expect(mockSyncOrchestratorService.configure).not.toHaveBeenCalled();
		expect(mockSyncOrchestratorService.executeSync).toHaveBeenCalled();
		expect(mockExit).toHaveBeenCalledWith(0);
	});

	test('should exit with error code 1 when credentials are missing', async () => {
		process.argv = ['node', 'sync-all.js'];

		try {
			await import('../sync-all.js?' + Date.now());
		} catch (error) {
			expect(error.message).toBe('process.exit() called');
		}

		expect(mockExit).toHaveBeenCalledWith(1);
	});

	test('should handle unknown arguments by showing help', async () => {
		process.argv = ['node', 'sync-all.js', '--unknown-flag'];

		try {
			await import('../sync-all.js?' + Date.now());
		} catch (error) {
			expect(error.message).toBe('process.exit() called');
		}

		expect(mockExit).toHaveBeenCalledWith(0);
		expect(consoleOutput.join('\n')).toContain('MetaExodus - Database Synchronization Tool');
	});
});
