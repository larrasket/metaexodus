# Handling Passwords with Special Characters

MetaExodus now properly handles passwords containing special characters. Here are examples of how to configure your `.env` file:

## Examples

### Basic Special Characters

```bash
# These special characters are now properly handled:
DB_REMOTE_PASSWORD=p@ssw0rd!@#$%^&*()_+-=[]{}|;:,.<>?
```

### Quoted Passwords (Recommended)

```bash
# For passwords with many special characters, use quotes:
DB_REMOTE_PASSWORD="p@ssw0rd!@#$%^&*()_+-=[]{}|;:,.<>?"
```

### Common Special Character Examples

```bash
# Password with @ symbol
DB_REMOTE_PASSWORD=user@domain.com

# Password with : symbol
DB_REMOTE_PASSWORD=pass:word

# Password with / symbol
DB_REMOTE_PASSWORD=path/to/password

# Password with ? symbol
DB_REMOTE_PASSWORD=what?is?this?

# Password with & symbol
DB_REMOTE_PASSWORD=pass&word

# Password with = symbol
DB_REMOTE_PASSWORD=pass=word

# Password with + symbol
DB_REMOTE_PASSWORD=pass+word

# Password with % symbol
DB_REMOTE_PASSWORD=pass%word

# Complex password with multiple special characters
DB_REMOTE_PASSWORD="MyP@ssw0rd!@#$%^&*()_+-=[]{}|;:,.<>?"
```

## What Changed

1. **URL Encoding**: Database connection strings now properly URL encode usernames and passwords
2. **Environment Validation**: The system validates that passwords are treated as strings
3. **Documentation**: Updated README and template files with guidance on special characters

## Testing

The system includes tests to verify special character handling:

```bash
npm test -- tests/config/database.test.js
```

This will run tests that verify:

- URL encoding of special characters in connection strings
- Proper handling of various special character combinations
- Environment validation with special characters

## Troubleshooting

If you encounter issues with special characters:

1. **Use quotes**: Wrap your password in quotes in the `.env` file
2. **Check encoding**: Ensure your `.env` file is saved with UTF-8 encoding
3. **Escape characters**: If needed, escape special characters according to your shell's requirements

## Supported Special Characters

The following special characters are now properly handled:

- `@` (at symbol)
- `:` (colon)
- `/` (forward slash)
- `?` (question mark)
- `#` (hash)
- `&` (ampersand)
- `=` (equals)
- `+` (plus)
- `%` (percent)
- `!` (exclamation)
- `$` (dollar)
- `^` (caret)
- `*` (asterisk)
- `()` (parentheses)
- `_` (underscore)
- `-` (hyphen)
- `=` (equals)
- `[]` (square brackets)
- `{}` (curly braces)
- `|` (pipe)
- `;` (semicolon)
- `,` (comma)
- `.` (period)
- `<` (less than)
- `>` (greater than)
