import logging
import secrets

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

from radiant.dags import DEFAULT_ARGS, IS_AWS, NAMESPACE, ECSEnv, load_docs_md

if IS_AWS:
    from radiant.dags.operators import ecs as operators
else:
    from radiant.dags.operators import k8s as operators

TOOLBOX_COMMANDS = ["create-tenant", "create-user", "refresh-tenants"]

_KV_ITEMS = {
    "type": "object",
    "properties": {"name": {"type": "string"}, "value": {"type": "string"}},
    "required": ["name", "value"],
}

logger = logging.getLogger(__name__)


def _resolve_env_vars(env_vars: list[dict]) -> dict[str, str]:
    return {item["name"]: item["value"] for item in env_vars}


def _generate_user_password(command: str, token_urlsafe=secrets.token_urlsafe) -> str:
    """A `create-user` run needs a fresh password every time -- unlike the deployment's
    baked DB/PG/Ranger/Keycloak-admin credentials, this can't live in the task
    definition. `create-user` only applies it when provisioning a *new* Keycloak user
    (`-email`); with `-sub` (an existing user) it's ignored (radiant-portal
    `backend/internal/service/admin.go` `ProvisionUser`), so generating one
    unconditionally here never resets an existing account.
    """
    if command != "create-user":
        return ""
    return token_urlsafe(18)


@dag(
    dag_id=f"{NAMESPACE}-toolbox",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["radiant", "toolbox", "manual"],
    dag_display_name="Radiant - Toolbox",
    doc_md=load_docs_md("toolbox.md"),
    render_template_as_native_obj=True,
    params={
        "command": Param(
            "create-tenant",
            type="string",
            enum=TOOLBOX_COMMANDS,
            title="Command",
            description="Toolbox binary to run.",
        ),
        "args": Param(
            [],
            type="array",
            items={"type": "string"},
            title="Arguments",
            description=(
                "CLI flags passed verbatim to the command, e.g. "
                '["-code", "demo", "-name", "Demo Hospital"] for create-tenant.'
            ),
        ),
        "env_vars": Param(
            [],
            type="array",
            items=_KV_ITEMS,
            title="Environment variables",
            description=(
                'Plain (non-secret) container env vars, e.g. [{"name": "RANGER_URL", '
                '"value": "http://ranger:6080"}]. The literal value is stored in the DAG '
                "run's history -- never put a secret here. There is no secret-injection "
                "param: bake a one-off credential into the deployment instead (the K8s "
                "secret / ECS task definition's Secrets Manager entries -- see the "
                "Credentials section below). The one exception is create-user's "
                "USER_PASSWORD, which this DAG generates and logs for you -- see below."
            ),
        ),
    },
)
def toolbox():
    @task(task_id="generate_user_password", task_display_name="[PyOp] Generate User Password")
    def generate_user_password(command: str) -> str:
        password = _generate_user_password(command)
        if password:
            logger.info(
                "Temporary password for the new user (share it out-of-band and have "
                "them change it on first login): %s",
                password,
            )
        return password

    password = generate_user_password(command="{{ params.command }}")

    if IS_AWS:

        @task(task_id="build_ecs_environment", task_display_name="[PyOp] Build ECS Environment")
        def build_ecs_environment(env_vars: list[dict], password: str) -> list[dict]:
            environment = list(env_vars)
            if password:
                environment.append({"name": "USER_PASSWORD", "value": password})
            return environment

        environment = build_ecs_environment(env_vars="{{ params.env_vars }}", password=password)
        operators.Toolbox.get_run_command(ecs_env=ECSEnv(), extra_env=environment)
    else:

        @task(task_id="resolve_env_vars", task_display_name="[PyOp] Resolve Env Vars")
        def resolve_env_vars(env_vars: list[dict], password: str) -> dict[str, str]:
            resolved = _resolve_env_vars(env_vars)
            if password:
                resolved["USER_PASSWORD"] = password
            return resolved

        extra_env = resolve_env_vars(env_vars="{{ params.env_vars }}", password=password)
        operators.Toolbox.get_run_command(extra_env=extra_env)


toolbox()
