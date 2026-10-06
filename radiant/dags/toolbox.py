import logging
import secrets

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

from radiant.dags import DEFAULT_ARGS, IS_AWS, NAMESPACE, RADIANT_LOCK_S3_BUCKET, ECSEnv, load_docs_md
from radiant.tasks.locking import IMPORT_MUTEX_LOCK_NAME, check_lock, describe_lock_status, release_lock

if IS_AWS:
    from radiant.dags.operators import ecs as operators
else:
    from radiant.dags.operators import k8s as operators

TOOLBOX_COMMANDS = ["create-tenant", "create-user", "update-user", "refresh-tenants", "check-lock"]

# DAG commands backed by a differently named binary in the toolbox image.
_TOOLBOX_BINARIES = {"update-user": "create-user"}

_KV_ITEMS = {
    "type": "object",
    "properties": {"name": {"type": "string"}, "value": {"type": "string"}},
    "required": ["name", "value"],
}

logger = logging.getLogger(__name__)


def _resolve_env_vars(env_vars: list[dict]) -> dict[str, str]:
    return {item["name"]: item["value"] for item in env_vars}


def _delete_if_expired(args: list[str]) -> bool:
    return "-delete-if-expired" in args


def _force_delete(args: list[str]) -> bool:
    return "-force-delete" in args


def _toolbox_command(command: str, args: list[str]) -> list[str]:
    """The container command line: the image binary for `command`, then `args` verbatim.

    `update-user` is `create-user -sub`. With `-sub`, create-user skips Keycloak entirely, so the
    account (a person, or a client's service-account user such as airflow) keeps its password,
    names and required actions; only Postgres grants, Ranger roles and the StarRocks user are
    (re)applied. With `-email` instead, create-user upserts the Keycloak user, which overwrites
    those attributes and resets the password whenever USER_PASSWORD is set -- hence `-sub` is
    mandatory here.
    """
    if command == "update-user" and "-sub" not in args:
        raise ValueError(
            'update-user needs "-sub", "<keycloak user id>" in args: it never touches Keycloak, '
            "so the user is identified by its sub, not its email"
        )
    return [_TOOLBOX_BINARIES.get(command, command), *args]


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
            description="Toolbox command to run.",
        ),
        "args": Param(
            [],
            type="array",
            items={"type": "string"},
            title="Arguments",
            description=(
                "CLI flags passed verbatim to the command, e.g. "
                '["-code", "demo", "-name", "Demo Hospital"] for create-tenant. update-user takes '
                'create-user\'s flags and requires "-sub". For check-lock, two '
                'flags are recognized. "-delete-if-expired": if the import_mutex lock is held and '
                "past its TTL, delete it; no effect on a lock still within its TTL. "
                '"-force-delete": delete it whatever its age and whoever holds it -- the only way to '
                "clear a lock a live run still holds, so confirm that run has finished first, or two "
                "imports can write to StarRocks and Iceberg at once. Run without flags to see the "
                "lock's holder and age before deciding."
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

    @task.branch(task_id="select_execution_path", task_display_name="[PyOp] Select Execution Path")
    def select_execution_path(command: str) -> str:
        if command == "check-lock":
            return "check_import_lock"
        return "resolve_toolbox_command"

    branch = select_execution_path(command="{{ params.command }}")

    @task(task_id="check_import_lock", task_display_name="[PyOp] Check Import Lock")
    def check_import_lock(args: list[str]):
        status = check_lock(bucket=RADIANT_LOCK_S3_BUCKET, name=IMPORT_MUTEX_LOCK_NAME)
        message, should_delete = describe_lock_status(
            status, _delete_if_expired(args), force_delete=_force_delete(args)
        )
        logger.info(message)
        if should_delete:
            release_lock(bucket=RADIANT_LOCK_S3_BUCKET, name=IMPORT_MUTEX_LOCK_NAME)

    check_lock_task = check_import_lock(args="{{ params.args }}")
    branch >> check_lock_task

    @task(task_id="resolve_toolbox_command", task_display_name="[PyOp] Resolve Toolbox Command")
    def resolve_toolbox_command(command: str, args: list[str]) -> list[str]:
        return _toolbox_command(command, args)

    toolbox_command = resolve_toolbox_command(command="{{ params.command }}", args="{{ params.args }}")
    branch >> toolbox_command

    if IS_AWS:

        @task(task_id="build_ecs_environment", task_display_name="[PyOp] Build ECS Environment")
        def build_ecs_environment(env_vars: list[dict], password: str) -> list[dict]:
            environment = list(env_vars)
            if password:
                environment.append({"name": "USER_PASSWORD", "value": password})
            return environment

        environment = build_ecs_environment(env_vars="{{ params.env_vars }}", password=password)
        toolbox_command >> environment
        operators.Toolbox.get_run_command(ecs_env=ECSEnv(), command=toolbox_command, extra_env=environment)
    else:

        @task(task_id="resolve_env_vars", task_display_name="[PyOp] Resolve Env Vars")
        def resolve_env_vars(env_vars: list[dict], password: str) -> dict[str, str]:
            resolved = _resolve_env_vars(env_vars)
            if password:
                resolved["USER_PASSWORD"] = password
            return resolved

        extra_env = resolve_env_vars(env_vars="{{ params.env_vars }}", password=password)
        toolbox_command >> extra_env
        operators.Toolbox.get_run_command(command=toolbox_command, extra_env=extra_env)


toolbox()
