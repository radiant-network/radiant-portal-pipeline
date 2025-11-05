# 📄 `load_data.sh` – PostgreSQL CSV Loader

This script automates the process of:

1. **Truncating** a set of interrelated PostgreSQL tables (with `CASCADE`)
2. **Importing CSV files** into those tables using the `psql \copy` command

---

## ✅ Requirements

- Bash shell (Linux/macOS)
- `psql` installed and accessible in your `PATH`
- PostgreSQL database with the expected schema already created
- A directory containing the CSV files (with headers)

---

## 🧩 Tables Affected

The following tables are truncated in the specified order (to respect foreign keys):

```
organization
patient
project
request
case_analysis
cases
family
obs_categorical
sample
experiment
sequencing_experiment
pipeline
task
task_has_sequencing_experiment
document
task_has_document
```

Each table will be loaded from its corresponding `.csv` file.

---

## 📥 Usage

```bash
./load_data.sh <PGUSER> <PGHOST> <PGPORT> <PGDATABASE> <CSV_DIR>
```

### Arguments

| Argument      | Description                                |
|---------------|--------------------------------------------|
| `PGUSER`      | Database user                              |
| `PGHOST`      | PostgreSQL hostname (e.g., `localhost`)    |
| `PGPORT`      | PostgreSQL port (usually `5432`)           |
| `PGDATABASE`  | Target database name                       |
| `CSV_DIR`     | Path to directory containing the CSV files |

### Password

- If the `PGPASSWORD` environment variable is **already set**, it will be used to authenticate.
- If not, the script will **prompt you securely** to enter the password at runtime.

---


## 🛠 Customization

You can adapt the script to:

- Add logging of errors or rows inserted
- Run specific validation after loading
- Use transactions around insertions (if not using `\copy` directly)
- Skip loading certain tables (by commenting out the relevant lines)

---

## 🧪 Testing Tip

To dry-run the file loading commands without executing:

```bash
bash -x ./load_data.sh ...
```
