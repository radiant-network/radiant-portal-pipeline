from airflow import DAG
from airflow.decorators import task

from radiant.dags import DEFAULT_ARGS, NAMESPACE

with DAG(
    dag_id=f"{NAMESPACE}-diagnostic",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["radiant", "starrocks", "manual", "dns"],
    dag_display_name="Radiant - Run Diagnostics",
) as dag:

    @task(task_id="check_starrocks_dns_ttl", task_display_name="[PyOp] Check StarRocks DNS TTL")
    def check_starrocks_dns_ttl():
        """
        Resolve the StarRocks connection host and log DNS TTLs.
        """
        import dns.resolver
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection("starrocks_conn")
        host = conn.host
        if not host:
            raise ValueError("starrocks_conn has no host set")

        resolver = dns.resolver.Resolver()
        resolver.lifetime = 10.0  # total timeout across retries
        resolver.timeout = 5.0  # per-query timeout

        # Resolve A + AAAA + CNAME chain. CNAME TTLs are surfaced automatically
        # by dnspython in the Answer.response.answer list (one RRset per hop).
        any_printed = False
        for rdtype in (dns.rdatatype.A, dns.rdatatype.AAAA):
            try:
                answer = resolver.resolve(host, rdtype, raise_on_no_answer=False)
            except dns.resolver.NXDOMAIN:
                raise RuntimeError(f"NXDOMAIN for {host}") from None
            except dns.exception.DNSException as e:
                print(f"{rdtype.name} lookup failed for {host}: {e}")
                continue

            for rrset in answer.response.answer:
                rtype = dns.rdatatype.to_text(rrset.rdtype)
                for item in rrset:
                    print(f"  {rrset.name.to_text()} TTL={rrset.ttl}s type={rtype} -> {item.to_text()}")
                    any_printed = True

        if not any_printed:
            print(f"No DNS records resolved for {host}")

    @task(task_id="check_starrocks_version", task_display_name="[PyOp] Check StarRocks Version")
    def check_starrocks_version():
        """Query StarRocks for its current version via `SELECT current_version()`."""
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection("starrocks_conn")
        with conn.get_hook().get_conn().cursor() as cursor:
            cursor.execute("SELECT current_version()")
            (version,) = cursor.fetchone()
            print(f"StarRocks current_version: {version}")

    check_starrocks_dns_ttl()
    check_starrocks_version()
