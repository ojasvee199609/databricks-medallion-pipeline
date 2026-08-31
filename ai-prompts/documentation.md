# AI prompts used for project documentation.

## Prompt: Create Repository Folder Structure

**PROMPT SENT:**
"Create the basic folder and file structure for a Git repository called
'databricks-medallion-pipeline'... [full structure provided]... After
creating the structure, output a tree view so I can confirm it matches
exactly."

**AI RESPONSE SUMMARY:**
Cursor generated the full folder scaffold with placeholder files, added
a `.gitignore` for Python/PySpark/Databricks artifacts, and printed a
tree view of the result.

**YOUR EVALUATION:**
✓ **What was good:**
- Tree output matched the required structure exactly — all 7 root-level
  docs, all 5 src/ subfolders with correct placeholder files, data/,
  database/, and all 7 ai-prompts/ activity files present and correctly
  named
- Added a `.gitignore` proactively (not in spec, but appropriate —
  excludes __pycache__, .venv, checkpoint dirs, etc.)

## Remaining documentation work

△ **What you verified (not just trusted):**
- Ran `tree` locally and diffed the output line-by-line against the
  required structure in the project brief before committing
- Confirmed all 7 ai-prompts/ files were created individually (not
  collapsed into one file, which is an easy shortcut AI sometimes takes)
