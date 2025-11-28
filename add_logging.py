import sys

# Read the file
with open('app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Check if error handlers already exist
if '@app.errorhandler(500)' in content:
    print("Error handlers already exist!")
    sys.exit(0)

# Find the last line with "if __name__ ==" 
lines = content.split('\n')
insert_index = -1

for i, line in enumerate(lines):
    if 'if __name__ == "__main__":' in line:
        insert_index = i
        break

if insert_index == -1:
    print("Could not find if __name__ == '__main__'")
    sys.exit(1)

# Find the end of the if __name__ block
end_index = insert_index
for i in range(insert_index + 1, len(lines)):
    if lines[i] and not lines[i].startswith(' ') and not lines[i].startswith('\t') and lines[i].strip():
        end_index = i
        break
else:
    end_index = len(lines)

# Add startup logging and error handlers
new_code = '''
# Startup logging when running under Gunicorn
if __name__ != "__main__":
    logging.info("=" * 60)
    logging.info("🚀 Application starting under Gunicorn")
    logging.info(f"Python version: {sys.version}")
    logging.info(f"Flask app name: {app.name}")
    logging.info(f"Environment variables:")
    logging.info(f"  - PORT: {os.environ.get('PORT', 'not set')}")
    logging.info(f"  - APP_SECRET: {'set' if os.environ.get('APP_SECRET') else 'NOT SET (using default)'}")
    logging.info(f"Working directory: {os.getcwd()}")
    logging.info("=" * 60)

# Error handlers for better logging
@app.errorhandler(500)
def internal_error(error):
    logging.error(f"Internal Server Error: {error}", exc_info=True)
    return "Internal Server Error - Check logs for details", 500

@app.errorhandler(Exception)
def handle_exception(e):
    logging.error(f"Unhandled exception: {e}", exc_info=True)
    return f"An error occurred: {str(e)}", 500
'''

# Insert the new code after the if __name__ block
lines.insert(end_index, new_code)

# Write back
with open('app.py', 'w', encoding='utf-8') as f:
    f.write('\n'.join(lines))

print("Successfully added error logging!")
