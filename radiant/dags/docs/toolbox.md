# Radiant Toolbox

Launches one command from the `radiant-portal` backend's `toolbox` image
(`radiant-portal/backend/toolbox.Dockerfile`) as a one-off ECS task or K8s pod, or (for
`check-lock` only) runs entirely inside this DAG with no external image at all. Pick
`command`, fill in `args` for that command (shown below as each command's own `--help`
would print it), hit trigger.

## create-tenant

```
Usage of create-tenant:
  -code string
    	tenant code (required); [a-z][a-z0-9_]*
  -dry-run
    	print the plan without applying anything
  -name string
    	tenant display name (required)
```

Onboard a tenant across Postgres, StarRocks, and Ranger. Idempotent — re-running
converges rather than duplicating (`radiant-portal/backend/cmd/create-tenant/README.md`).

`args`: `["-code", "demo", "-name", "Demo Hospital"]` (add `"-dry-run"` to preview first).

## create-user

```
Usage of create-user:
  -email string
    	user email (PK and Keycloak username); required unless -sub is given
  -first string
    	first name
  -grant value
    	tenant:org:role grant; repeatable
  -last string
    	last name
  -sub string
    	existing Keycloak sub (user_id); skips Keycloak and provisions Postgres/Ranger/StarRocks only
```

Provision a user across Keycloak, Postgres, Ranger, and StarRocks, keyed on the Keycloak
`sub` (`radiant-portal/backend/cmd/create-user/main.go`). Exactly one of `-email` /
`-sub` is required.

`args`: `["-email", "user@example.org", "-first", "Carol", "-last", "Demo", "-grant",
"demo:*:geneticist"]` (repeat `-grant` per grant; use `-sub <keycloak-sub>` instead of
`-email` for a user that already exists in Keycloak). Don't pass the CLI's own `-p`
(prompt-for-password) flag here — see USER_PASSWORD below, this DAG always sets it, and
`-p` is ignored whenever that env var is set.

## refresh-tenants

```
Usage of refresh-tenants:
  -code string
    	tenant code to refresh; refreshes all tenants if omitted
```

Re-apply StarRocks views + Ranger masking policies — the break-glass command after a
schema or Ranger policy change (`radiant-portal/backend/cmd/refresh-tenants/README.md`).

`args`: `["-code", "demo"]`, or `[]` to refresh every tenant.

## check-lock

```
Usage of check-lock:
  -delete-if-expired
    	delete the import_mutex lock, but only if it is also past its 6h TTL
```

Not part of the toolbox image at all -- runs entirely in this DAG (a plain S3 read, and
optionally a delete), never touching ECS/K8s or the toolbox binary. Reports the
`import_mutex` lock's holder and age (design/SJRA-1811-opendatalake-integration.md §4).
`import_part` and the (future) re-annotation DAG use this lock to keep from writing to
StarRocks at the same time; a failed `import_part` run leaves its lock in place on
purpose (see the design doc), so an abandoned lock needs a deliberate operator decision
to clear, not an automatic one. A lock still within its TTL is never deleted, regardless
of the flag.

`args`: `[]` (the default) just reports the lock's holder and age, no side effect.
`["-delete-if-expired"]` additionally deletes it once both conditions hold. Run once
without the flag to see the lock's status before deciding whether to pass it.

## Params

- `command` — which of the four commands above to run.
- `args` — CLI flags for `command`, per the `--help` blocks above.
- `env_vars` — plain, non-secret container env vars: `[{"name": ..., "value": ...}]`.
  The value is stored as-is in the DAG run's history — never put a secret here. There
  is no secret-injection param: a one-off credential change goes into the deployment
  itself (see Credentials below), not through a DAG run. Ignored by `check-lock`.

## Credentials

Credentials (`DB_*`/`PG*` for StarRocks and Postgres, `RANGER_ADMIN_PASSWORD`,
`KEYCLOAK_ADMIN_CLIENT_SECRET`, etc.) are not passed through DAG params at all. The
container reads them the same way the `radiant-portal` API does, supplied by the
deployment: a K8s secret (`RADIANT_TOOLBOX_OPERATOR_SECRET_NAME`, default
`radiant-toolbox-secret`) on K8s, or Secrets Manager entries baked into the ECS task
definition (`RADIANT_TOOLBOX_TASK_DEFINITION`) on AWS -- see
`radiant-portal-deployment/deployment/terraform/app/toolbox.tf` for the exact set. A
one-off change to one of these means updating that secret / task definition directly,
then triggering the DAG.

`create-user`'s `USER_PASSWORD` is the one exception: it's a fresh value on every run
(the password assigned to the new user), not a fixed deployment credential, so it
can't be baked in. This DAG generates a random one for you whenever `command` is
`create-user`, and logs it on the `generate_user_password` task -- copy it from there
and share it with the user out-of-band; have them change it on first login. Passing
`-sub <keycloak-sub>` instead of `-email` (an already-existing Keycloak user) makes
`create-user` ignore the generated password entirely.
