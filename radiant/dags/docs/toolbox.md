# Radiant Toolbox

Launches one command from the `radiant-portal` backend's `toolbox` image
(`radiant-portal/backend/toolbox.Dockerfile`) as a one-off ECS task or K8s pod.
The image bundles three control-plane CLIs with no fixed entrypoint:

- **create-tenant** — onboard a tenant across Postgres, StarRocks, and Ranger.
  `args`: `["-code", "demo", "-name", "Demo Hospital"]` (add `"-dry-run"` to preview).
- **create-user** — provision a user across Keycloak, Postgres, Ranger, and StarRocks.
  `args`: `["-email", "user@example.org", "-first", "Carol", "-last", "Demo",
  "-grant", "demo:*:geneticist"]` (repeat `-grant` per grant; use `-sub <keycloak-sub>`
  instead of `-email` for a user that already exists in Keycloak).
- **refresh-tenants** — re-apply StarRocks views + Ranger masking policies.
  `args`: `["-code", "demo"]`, or `[]` to refresh every tenant.

## Params

- `command` — which binary to run (`create-tenant` / `create-user` / `refresh-tenants`).
- `args` — CLI flags passed verbatim to `command`.
- `env_vars` — plain, non-secret container env vars: `[{"name": ..., "value": ...}]`.
  The value is stored as-is in the DAG run's history — never put a secret here. There
  is no secret-injection param: a one-off credential change goes into the deployment
  itself (see Credentials below), not through a DAG run.

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
