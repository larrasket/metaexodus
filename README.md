# MetaExodus

API-powered migration from Metabase to local Postgres

https://github.com/user-attachments/assets/33f7f758-98ab-4825-8a42-6cb0803a0387

MetaExodus is a simple tool that copies your entire database from Metabase to
your local PostgreSQL. It connects to Metabase via API, grabs all your tables
and data, then recreates everything locally. Perfect for developers who need a
local copy of production data for testing and development.

## Requirements

- Node.js 16+
- PostgreSQL running locally (database will be created automatically)
- Access to a Metabase instance

## Quick Start

### 1. Install

```bash
git clone https://github.com/larrasket/metaexodus
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
# For passwords with special characters, use quotes: DB_REMOTE_PASSWORD="your_password_with_special_chars"
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

## Finding your Metabase Database ID

1. Go to your Metabase admin panel
2. Click on "Databases"
3. Find your database in the list
4. The ID is in the URL when you click on it: `/admin/databases/4` means ID is `4`

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

## TODO

- [ ] Incremental sync: Only copy new/changed data instead of everything
- [ ] Schema-only mode: Just copy table structures without data
- [ ] Selective table sync: Choose which tables to copy
- [ ] Resume interrupted syncs: Continue where you left off if something fails
- [ ] Multiple database support: MySQL, SQLite, etc.

### Current Limitations

- Auto-creates the database and ensures schema; complex nested structures are stored as JSONB
- Only works with PostgreSQL locally
- No way to exclude sensitive tables or columns
