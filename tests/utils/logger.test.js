import { jest } from '@jest/globals';
import { Logger } from '../../src/utils/logger.js';

describe('Logger', () => {
  let logger;
  let consoleSpy;

  beforeEach(() => {
    logger = new Logger();
    consoleSpy = jest.spyOn(console, 'log').mockImplementation();
    jest.clearAllMocks();
  });

  afterEach(() => {
    consoleSpy.mockRestore();
    // Clean up any remaining spinners and progress bars
    if (logger) {
      logger.cleanup();
    }
  });

  describe('basic logging methods', () => {
    test('should have winston logger instance', () => {
      expect(logger.winston).toBeDefined();
      expect(typeof logger.winston.info).toBe('function');
      expect(typeof logger.winston.error).toBe('function');
      expect(typeof logger.winston.warn).toBe('function');
      expect(typeof logger.winston.debug).toBe('function');
    });

    test('should not throw when calling logging methods', () => {
      expect(() => logger.info('Test info message')).not.toThrow();
      expect(() => logger.error('Test error message')).not.toThrow();
      expect(() => logger.warn('Test warning message')).not.toThrow();
      expect(() => logger.debug('Test debug message')).not.toThrow();
    });

    test('should handle logging with data objects', () => {
      const data = { key: 'value' };
      expect(() => logger.info('Test message', data)).not.toThrow();
      expect(() => logger.warn('Test message', data)).not.toThrow();
    });

    test('should handle error objects', () => {
      const error = new Error('Test error');
      expect(() => logger.error('Test message', error)).not.toThrow();
    });

    test('should log success messages to console', () => {
      logger.success('Test success message');
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Test success message')
      );
    });
  });

  describe('colorizeLevel', () => {
    test('should colorize different log levels', () => {
      expect(logger.colorizeLevel('error')).toBeDefined();
      expect(logger.colorizeLevel('warn')).toBeDefined();
      expect(logger.colorizeLevel('info')).toBeDefined();
      expect(logger.colorizeLevel('debug')).toBeDefined();
    });

    test('should handle unknown log levels', () => {
      expect(logger.colorizeLevel('unknown')).toBe('UNKNOWN');
    });
  });

  describe('spinner functionality', () => {
    test('should start and stop spinner', () => {
      const spinner = logger.startSpinner('Loading...');
      expect(spinner).toBeDefined();
      expect(logger.spinners.has('default')).toBe(true);

      logger.stopSpinner(true, 'Success!');
      expect(logger.spinners.has('default')).toBe(false);
    });

    test('should handle multiple spinners with different IDs', () => {
      logger.startSpinner('Loading 1...', 'spinner1');
      logger.startSpinner('Loading 2...', 'spinner2');

      expect(logger.spinners.has('spinner1')).toBe(true);
      expect(logger.spinners.has('spinner2')).toBe(true);

      logger.stopSpinner(true, 'Success 1!', 'spinner1');
      expect(logger.spinners.has('spinner1')).toBe(false);
      expect(logger.spinners.has('spinner2')).toBe(true);
    });

    test('should update spinner text', () => {
      logger.startSpinner('Initial text');
      logger.updateSpinner('Updated text');

      const spinner = logger.spinners.get('default');
      expect(spinner.text).toContain('Updated text');

      // Clean up the spinner to prevent hanging
      logger.stopSpinner(true);
    });

    test('should handle stopping non-existent spinner', () => {
      expect(() =>
        logger.stopSpinner(true, 'Success!', 'nonexistent')
      ).not.toThrow();
    });
  });

  describe('progress bar functionality', () => {
    test('should create and manage progress bar', () => {
      const bar = logger.createProgressBar(100, 'Processing');
      expect(bar).toBeDefined();
      expect(logger.progressBars.has('default')).toBe(true);

      logger.updateProgress(50);
      logger.stopProgress();
      expect(logger.progressBars.has('default')).toBe(false);
    });

    test('should handle multiple progress bars', () => {
      logger.createProgressBar(100, 'Task 1', 'bar1');
      logger.createProgressBar(200, 'Task 2', 'bar2');

      expect(logger.progressBars.has('bar1')).toBe(true);
      expect(logger.progressBars.has('bar2')).toBe(true);

      logger.stopProgress('bar1');
      expect(logger.progressBars.has('bar1')).toBe(false);
      expect(logger.progressBars.has('bar2')).toBe(true);
    });

    test('should handle updating non-existent progress bar', () => {
      expect(() =>
        logger.updateProgress(50, 'text', 'nonexistent')
      ).not.toThrow();
    });
  });

  describe('formatting methods', () => {
    test('should create section headers', () => {
      logger.section('Test Section');
      expect(consoleSpy).toHaveBeenCalledTimes(3);
    });

    test('should create subsection headers', () => {
      logger.subsection('Test Subsection');
      expect(consoleSpy).toHaveBeenCalledTimes(2);
    });

    test('should create tables', () => {
      const data = [
        { name: 'John', age: 30 },
        { name: 'Jane', age: 25 }
      ];

      logger.table(data);
      expect(consoleSpy).toHaveBeenCalled();
    });

    test('should handle empty table data', () => {
      logger.table([]);
      expect(consoleSpy).not.toHaveBeenCalled();
    });

    test('should create summary', () => {
      const stats = {
        totalTables: 10,
        syncedTables: 8,
        totalRows: 1000
      };

      logger.summary(stats);
      expect(consoleSpy).toHaveBeenCalled();
    });
  });

  describe('cleanup functionality', () => {
    test('should cleanup all spinners and progress bars', () => {
      logger.startSpinner('Test spinner');
      logger.createProgressBar(100, 'Test progress');

      expect(logger.spinners.size).toBe(1);
      expect(logger.progressBars.size).toBe(1);

      logger.cleanup();

      expect(logger.spinners.size).toBe(0);
      expect(logger.progressBars.size).toBe(0);
    });

    test('should handle cleanup when no spinners or progress bars exist', () => {
      expect(() => logger.cleanup()).not.toThrow();
    });
  });
});
