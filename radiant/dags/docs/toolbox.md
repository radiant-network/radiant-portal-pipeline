# Radiant Toolbox

Runs one maintenance command, picked from the **command** param. Fill in **args** for that
command, hit trigger.

| Command | What it does | Runs where |
|:--|:--|:--|
| **create-tenant** | Onboards a tenant across Postgres, StarRocks and Ranger | Toolbox image |
| **create-user** | Provisions a user across Keycloak, Postgres, Ranger and StarRocks | Toolbox image |
| **refresh-tenants** | Re-applies StarRocks views and Ranger masking policies | Toolbox image |
| **check-lock** | Reports, and optionally clears, the import mutex | This DAG |

The three image-backed commands launch a one-off ECS task or K8s pod from the radiant-portal
backend's toolbox image (radiant-portal/backend/toolbox.Dockerfile). **check-lock** touches
neither — it is a plain S3 read and an optional delete, run inside this DAG.

The flag tables below are what each command's own --help would print.

### How a run flows

1. **generate_user_password** — mints a fresh password, but only when **command** is
   create-user. Otherwise it returns nothing.
2. **select_execution_path** — branches on **command**:
   - **check-lock** goes to **check_import_lock** and stops there. No container, no image.
   - The other three resolve their env vars, then run the command on ECS or K8s.

---

## create-tenant

| Flag | Type | Required | Effect |
|:--|:--|:--|:--|
| **-code** | string | Yes | Tenant code. A lowercase letter, then lowercase letters, digits or underscores |
| **-name** | string | Yes | Tenant display name |
| **-dry-run** | switch | No | Print the plan without applying anything |

Idempotent — re-running converges rather than duplicating. See
radiant-portal/backend/cmd/create-tenant/README.md.

**args**: "-code", "demo", "-name", "Demo Hospital" — add "-dry-run" to preview first.

---

## create-user

| Flag | Type | Required | Effect |
|:--|:--|:--|:--|
| **-email** | string | One of the two | User email. Both the primary key and the Keycloak username |
| **-sub** | string | One of the two | An existing Keycloak sub (user_id). Skips Keycloak; provisions Postgres, Ranger and StarRocks only |
| **-first** | string | No | First name |
| **-last** | string | No | Last name |
| **-grant** | string, repeatable | No | A tenant:org:role grant |

Keyed on the Keycloak sub. See radiant-portal/backend/cmd/create-user/main.go.

**args**: "-email", "user@example.org", "-first", "Carol", "-last", "Demo", "-grant",
"demo:\*:geneticist" — repeat -grant once per grant.

> Do not pass the CLI's own **-p** (prompt-for-password) flag. This DAG always sets
> USER_PASSWORD, and -p is ignored whenever that variable is set. See **Credentials** below.

---

## refresh-tenants

| Flag | Type | Required | Effect |
|:--|:--|:--|:--|
| **-code** | string | No | Tenant code to refresh. Refreshes every tenant if omitted |

The break-glass command after a schema or Ranger policy change. See
radiant-portal/backend/cmd/refresh-tenants/README.md.

**args**: "-code", "demo" — or nothing at all to refresh every tenant.

---

## check-lock

| args | Effect |
|:--|:--|
| *none* | Report the lock's holder and age. No side effect |
| **-delete-if-expired** | Also delete it, but only once it is held **and** past its 6h TTL |
| **-force-delete** | Delete it whatever its age, and whoever holds it |

Reports the **import_mutex** lock's holder and age. Both **radiant-import-part** and the
re-annotation DAG take this lock to keep from writing to StarRocks at the same time, so an
abandoned lock is cleared by a deliberate operator decision, never automatically. Design:
design/SJRA-1811-opendatalake-integration.md, section 4.

**Run with no args first.** The report is what tells you which flag, if either, is the right
one:

- **Held, inside its TTL** — a run is probably still working. -delete-if-expired will not
  touch it. Only -force-delete can, and only once you know better.
- **Held, past its TTL** — the usual abandoned-lock case. -delete-if-expired clears it.
- **Not held** — nothing to do; neither flag has any effect.

> **-force-delete is the only flag that can clear a lock a live run still holds.** That is
> exactly why it is separate from -delete-if-expired rather than an extension of it.
> Releasing the mutex out from under a running import or re-annotation lets the next import
> write to StarRocks and Iceberg alongside it — the thing the lock exists to prevent.
> Confirm the reported holder's DAG run really has finished.

The normal reason to reach for it is a holder that died without releasing — a killed worker,
a cleared task — where waiting out the remaining TTL is not worth it.

---

## Params

| Param | Contents |
|:--|:--|
| **command** | Which of the four commands above to run |
| **args** | CLI flags for that command, per the tables above |
| **env_vars** | Plain, non-secret container env vars: a list of objects with a name and a value. Ignored by check-lock |

> The **env_vars** values are stored as-is in the DAG run's history — never put a secret
> there. There is no secret-injection param by design; see below.

---

## Credentials

Credentials are not passed through DAG params at all. The container reads them the same way
the radiant-portal API does, supplied by the deployment:

| Platform | Source | Set by |
|:--|:--|:--|
| K8s | A K8s secret | **RADIANT_TOOLBOX_OPERATOR_SECRET_NAME**, default radiant-toolbox-secret |
| AWS | Secrets Manager entries in the ECS task definition | **RADIANT_TOOLBOX_TASK_DEFINITION** |

That covers the DB and PG variables for StarRocks and Postgres,
**RANGER_ADMIN_PASSWORD**, **KEYCLOAK_ADMIN_CLIENT_SECRET** and the rest — see
radiant-portal-deployment/deployment/terraform/app/toolbox.tf for the exact set. A one-off
change means updating that secret or task definition directly, then triggering the DAG.

**USER_PASSWORD** is the one exception. It is a fresh value every run — the password
assigned to the new user — not a fixed deployment credential, so it cannot be baked in:

1. Whenever **command** is create-user, this DAG generates a random one.
2. It is logged on the **generate_user_password** task. Copy it from there.
3. Share it with the user out-of-band, and have them change it on first login.

Passing -sub instead of -email, for an already-existing Keycloak user, makes create-user
ignore the generated password entirely.
