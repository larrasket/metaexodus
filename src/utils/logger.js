import chalk from 'chalk';
import { SingleBar } from 'cli-progress';
import ora from 'ora';
import {
  format as _format,
  transports as _transports,
  createLogger
} from 'winston';

const { gray, red, yellow, blue, green, cyan, bold } = chalk;

class Logger {
  constructor() {
    this.winston = createLogger({
      level: process.env.LOG_LEVEL || 'info',
      format: _format.combine(
        _format.timestamp(),
        _format.errors({ stack: true }),
        _format.printf(({ timestamp, level, message, stack }) => {
          const coloredLevel = this.colorizeLevel(level);
          const time = gray(timestamp.split('T')[1].split('.')[0]);
          return `${time} ${coloredLevel} ${message}${stack ? `\n${red(stack)}` : ''}`;
        })
      ),
      transports: [new _transports.Console()]
    });

    this.progressBars = new Map();
    this.spinners = new Map();
  }

  colorizeLevel(level) {
    const colors = {
      error: red.bold,
      warn: yellow.bold,
      info: blue.bold,
      debug: gray.bold
    };
    return colors[level]
      ? colors[level](level.toUpperCase())
      : level.toUpperCase();
  }

  info(message, data = null) {
    this.winston.info(data ? `${message} ${JSON.stringify(data)}` : message);
  }

  error(message, error = null) {
    if (error instanceof Error) {
      this.winston.error(message, error);
    } else {
      this.winston.error(message);
    }
  }

  warn(message, data = null) {
    this.winston.warn(data ? `${message} ${JSON.stringify(data)}` : message);
  }

  debug(message, data = null) {
    this.winston.debug(data ? `${message} ${JSON.stringify(data)}` : message);
  }

  success(message) {
    console.log(green(`✓ ${message}`));
  }

  startSpinner(text, id = 'default') {
    const spinner = ora({
      text: cyan(text),
      spinner: 'dots'
    }).start();
    this.spinners.set(id, spinner);
    return spinner;
  }

  updateSpinner(text, id = 'default') {
    const spinner = this.spinners.get(id);
    if (spinner) {
      spinner.text = cyan(text);
    }
  }

  stopSpinner(success = true, text = null, id = 'default') {
    const spinner = this.spinners.get(id);
    if (spinner) {
      if (success) {
        spinner.succeed(text ? green(text) : undefined);
      } else {
        spinner.fail(text ? red(text) : undefined);
      }
      this.spinners.delete(id);
    }
  }

  createProgressBar(total, label = 'Progress', id = 'default') {
    const bar = new SingleBar({
      format: `${cyan(label)} |${cyan('{bar}')}| {percentage}% | {value}/{total} | ETA: {eta}s `,
      barCompleteChar: '█',
      barIncompleteChar: '░',
      hideCursor: true
    });

    bar.start(total, 0);
    this.progressBars.set(id, bar);
    return bar;
  }

  updateProgress(current, text = null, id = 'default') {
    const bar = this.progressBars.get(id);
    if (bar) {
      bar.update(current);
      if (text) {
        bar.updateETA();
      }
    }
  }

  stopProgress(id = 'default') {
    const bar = this.progressBars.get(id);
    if (bar) {
      bar.stop();
      this.progressBars.delete(id);
    }
  }

  section(title) {
    const line = '═'.repeat(60);
    console.log(blue.bold(`\n${line}`));
    console.log(blue.bold(title.toUpperCase()));
    console.log(blue.bold(`${line}\n`));
  }

  subsection(title) {
    console.log(yellow.bold(`\n${title}`));
    console.log(yellow('─'.repeat(title.length)));
  }

  table(data, headers = null) {
    if (!data || data.length === 0) {
      return;
    }

    const keys = headers || Object.keys(data[0]);
    const maxWidths = keys.map((key) =>
      Math.max(key.length, ...data.map((row) => String(row[key] || '').length))
    );

    const headerRow = keys
      .map((key, i) => bold(key.padEnd(maxWidths[i])))
      .join(' | ');

    const separator = maxWidths.map((width) => '─'.repeat(width)).join('─┼─');

    console.log(headerRow);
    console.log(separator);

    data.forEach((row) => {
      const dataRow = keys
        .map((key, i) => String(row[key] || '').padEnd(maxWidths[i]))
        .join(' | ');
      console.log(dataRow);
    });
  }

  summary(stats) {
    this.section('SYNC SUMMARY');

    const summaryData = Object.entries(stats).map(([key, value]) => ({
      Metric: key
        .replace(/([A-Z])/g, ' $1')
        .replace(/^./, (str) => str.toUpperCase()),
      Value: typeof value === 'number' ? value.toLocaleString() : value
    }));

    this.table(summaryData);
    console.log();
  }

  cleanup() {
    this.spinners.forEach((spinner) => spinner.stop());
    this.progressBars.forEach((bar) => bar.stop());
    this.spinners.clear();
    this.progressBars.clear();
  }
}

const logger = new Logger();

process.on('SIGINT', () => {
  logger.cleanup();
  process.exit(0);
});

export { Logger, logger };
