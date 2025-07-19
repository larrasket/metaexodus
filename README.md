# MetaExodus

API-powered migration from Metabase to local Postgres

## What is this?

MetaExodus is a simple tool that copies your entire database from Metabase to your local PostgreSQL. It connects to Metabase via API, grabs all your tables and data, then recreates everything locally.

Perfect for developers who need a local copy of production data for testing and development.

## What it does

- Connects to your Metabase instance
- Downloads all table structures and data
- Creates identical tables in your local PostgreSQL
- Shows progress with nice terminal output
- Handles errors gracefully with retries

## Requirements

- Node.js 16+
- PostgreSQL running locally
- Access to a Metabase instance
- Yarn or npm

## Quick Start

### 1. Install

```bash
git clone <this-repo>
cd metaexodus
yarn install
```

### 2. Configure

Copy the template and fill in your details:

```bash
cp .env.template .env
```

Edit `.env` with your information:

```bash
# Your local PostgreSQL database
DB_LOCAL_HOST=localhost
DB_LOCAL_PORT=5432
DB_LOCAL_NAME=my_local_db
DB_LOCAL_USERNAME=postgres
DB_LOCAL_PASSWORD=your_password

# Your Metabase instance
METABASE_BASE_URL=https://your-metabase.com
METABASE_DATABASE_ID=1
DB_REMOTE_USERNAME=your_metabase_email
DB_REMOTE_PASSWORD=your_metabase_password

# Optional settings
DB_LOCAL_SSL=false
DB_BATCH_SIZE=1000
```

### 3. Run

```bash
yarn start
```

That's it! Watch as it copies everything over.

## How it works

MetaExodus does a complete database migration in these steps:

1. **Connects to Metabase** - Uses your login credentials to access the API
2. **Wipes your local database** - Clears everything to start fresh (be careful!)
3. **Discovers all tables** - Finds every table in your Metabase database
4. **Creates table structures** - Builds the same tables locally with proper data types
5. **Copies all data** - Downloads everything in batches and inserts it locally
6. **Shows you a summary** - Reports how many tables and rows were copied

The whole process usually takes a few minutes depending on your data size.

## Configuration Options

| Variable | What it does | Example |
|----------|-------------|---------|
| `DB_LOCAL_HOST` | Your PostgreSQL server | `localhost` |
| `DB_LOCAL_PORT` | PostgreSQL port | `5432` |
| `DB_LOCAL_NAME` | Local database name | `my_dev_db` |
| `DB_LOCAL_USERNAME` | PostgreSQL username | `postgres` |
| `DB_LOCAL_PASSWORD` | PostgreSQL password | `mypassword` |
| `METABASE_BASE_URL` | Your Metabase URL | `https://metabase.company.com` |
| `METABASE_DATABASE_ID` | Database ID in Metabase | `1` (check Metabase admin) |
| `DB_REMOTE_USERNAME` | Your Metabase login | `john@company.com` |
| `DB_REMOTE_PASSWORD` | Your Metabase password | `your_password` |

### Optional Settings

| Variable | What it does | Default |
|----------|-------------|---------|
| `DB_LOCAL_SSL` | Use SSL for local connection | `false` |
| `DB_BATCH_SIZE` | How many rows to copy at once | `1000` |
| `DB_CONNECTION_TIMEOUT` | Connection timeout in ms | `30000` |
| `SYNC_LOG_LEVEL` | How much logging to show | `info` |

## Finding your Metabase Database ID

1. Go to your Metabase admin panel
2. Click on "Databases" 
3. Find your database in the list
4. The ID is in the URL when you click on it: `/admin/databases/4` means ID is `4`

## Troubleshooting

**"Authentication failed"**
- Double-check your Metabase username and password
- Make sure you can log into Metabase normally

**"Connection timeout"**
- Your Metabase might be slow, try increasing `DB_CONNECTION_TIMEOUT`
- Check if your Metabase URL is correct

**"Database does not exist"**
- Make sure your local PostgreSQL database exists
- Create it first: `createdb my_dev_db`

**"Out of memory"**
- Reduce `DB_BATCH_SIZE` to something smaller like `500`

## Development

Want to contribute or modify the code?

```bash
# Run tests
yarn test

# Check code style
yarn lint

# Fix code style issues
yarn lint:fix
```

## TODO - Features we'd like to add

### High Priority
- [ ] **Incremental sync** - Only copy new/changed data instead of everything
- [ ] **Schema-only mode** - Just copy table structures without data
- [ ] **Selective table sync** - Choose which tables to copy
- [ ] **Resume interrupted syncs** - Continue where you left off if something fails

### Medium Priority  
- [ ] **Multiple database support** - MySQL, SQLite, etc.
- [ ] **Data filtering** - Copy only recent data (last 30 days, etc.)
- [ ] **Compression** - Compress data during transfer for speed
- [ ] **Parallel table processing** - Copy multiple tables at once

### Nice to Have
- [ ] **Web UI** - Browser interface instead of command line
- [ ] **Scheduled syncs** - Run automatically on a schedule
- [ ] **Data anonymization** - Scramble sensitive data during copy
- [ ] **Sync verification** - Double-check that everything copied correctly
- [ ] **Docker support** - Run the whole thing in a container
- [ ] **Progress persistence** - Save progress to disk so you can see it later

### Current Limitations
- Requires existing database schema (can't create the database for you)
- Wipes the entire local database each time (no incremental updates)
- Only works with PostgreSQL locally
- Only works with Metabase (no direct database connections)
- No way to exclude sensitive tables or columns

## License

GPL-3.0-only

## Questions?

Open an issue on GitHub if you run into problems or have ideas for improvements.