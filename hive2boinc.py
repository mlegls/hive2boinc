#!/usr/bin/env python
import asyncio
import os
import sys
import uuid

from MySQLdb import _mysql


# config
MYSQL_USER = "boincadm"
MYSQL_PASS = "boincpass"
MYSQL_DB = "test"
APP_NAME = "hive_test"
RESULTS_DIR = "hive_results"

# script
db = _mysql.connect(
    host="localhost", user=MYSQL_USER, password=MYSQL_PASS, database=MYSQL_DB
)


async def sh(cmd) -> tuple[str, str, int]:
    p = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    out = err = ""
    async for line in p.stdout:
        decoded = line.decode()
        print(decoded, end="")
        out += decoded
    async for line in p.stderr:
        decoded = line.decode()
        print(decoded, end="")
        err += decoded

    return out, err, await p.wait()


def escape_sq(text):
    return text.replace("'", "\\'")


if __name__ == "__main__":
    print("running hive job")
    id = "hive_" + str(uuid.uuid4())
    db.query(
        f"""
                insert into workunit (
                    create_time, appid, name, batch, 
                    rsc_fpops_est, rsc_fpops_bound, rsc_memory_bound, rsc_disk_bound, rsc_bandwidth_bound, 
                    need_validate, canonical_resultid, canonical_credit, transition_time, delay_bound, error_mask, file_delete_state, assimilate_state, hr_class, opaque, 
                    min_quorum, target_nresults, max_error_results, max_total_results, max_success_results, 
                    result_template_file, priority, fileset_id, app_version_id, transitioner_flags, size_class, keywords, app_version_num
                )
                values (
                    UNIX_TIMESTAMP(), (select id from app where name = '{APP_NAME}'), '{id}', 0, 
                    0.0, 0.0, 0.0, 0.0, 0.0, 
                    0, 0, 0.0, 0, 0, 0, 0, 0, 0, 0.0, 
                    1, 1, 1, 1, 1, 
                    '', 0, 0, 0, 0, 0, 'hive', 1
                )"""
    )
    out, err, code = asyncio.run(sh(f"hive run {' '.join(sys.argv[1:])}"))

    if "✅  Results accepted. Downloading result..." in out:
        # TODO make less fragile; maybe implement json-mode for hive run
        ipfs_addr = out.splitlines()[-1].split("/")[-1]
        res_dir = out.splitlines()[-4].split(" ")[-1]
        res_out, res_err = f"{res_dir}/stdout", f"{res_dir}/stderr"

        print("job succeeded")
        print(f"IPFS address: {ipfs_addr}")
        with open(res_out, "r") as f:
            print(f.read())

        os.makedirs(RESULTS_DIR, exist_ok=True)
        os.rename(res_dir, f"{RESULTS_DIR}/{id}")

        db.query(
            f"""
                    insert into result (
                        create_time, workunitid, appid, name,
                        server_state, client_state, outcome, hostid, userid,
                        report_deadline, sent_time, received_time, cpu_time, elapsed_time,
                        stderr_out,
                        batch, file_delete_state, validate_state, claimed_credit, granted_credit,
                        opaque, random, app_version_num, app_version_id, exit_status, teamid, priority,
                        flops_estimate, runtime_outlier, size_class, peak_working_set_size, peak_swap_size, peak_disk_usage
                    )
                    values (
                        UNIX_TIMESTAMP(), (select id from workunit where name = '{id}'), (select id from app where name = '{APP_NAME}'), '{id}',
                        5, 5, 1, 0, 0,
                        0, 0, 0, 0.0, 0.0,
                        '<![CDATA[<stderr_txt>{escape_sq(err)}</stderr_txt><stdout_txt>{escape_sq(out)}</stdout_txt>]]>',
                        0, 2, 1, 0, 0,
                        0, 0, 0, 0, 0, 0, 0,
                        0.0, 0, 0, 0.0, 0.0, 0.0
                    )"""
        )
        db.query(
            f"update workunit set canonical_resultid = (select id from result where name = '{id}') where name = '{id}'"
        )

        sys.exit(0)
    else:
        print("job failed")

        db.query(
            f"""
                    insert into result (
                        create_time, workunitid, appid, name,
                        server_state, client_state, outcome, hostid, userid,
                        report_deadline, sent_time, received_time, cpu_time, elapsed_time,
                        stderr_out,
                        batch, file_delete_state, validate_state, claimed_credit, granted_credit,
                        opaque, random, app_version_num, app_version_id, exit_status, teamid, priority,
                        flops_estimate, runtime_outlier, size_class, peak_working_set_size, peak_swap_size, peak_disk_usage
                    )
                    values (
                        UNIX_TIMESTAMP(), (select id from workunit where name = '{id}'), (select id from app where name = '{APP_NAME}'), '{id}',
                        5, 3, 3, 0, 0,
                        0, 0, 0, 0.0, 0.0,
                        '<![CDATA[<stderr_txt>{escape_sq(err)}</stderr_txt><stdout_txt>{escape_sq(out)}</stdout_txt>]]]>',
                        0, 2, 1, 0, 0,
                        0, 0, 0, 0, 255, 0, 0,
                        0.0, 0, 0, 0.0, 0.0, 0.0
                    )"""
        )
        db.query(
            f"update workunit set canonical_resultid = (select id from result where name = '{id}') where name = '{id}'"
        )

        sys.exit(1)


# TODO potential improvements
# - update workunit and result before `hive run` and as it runs, rather than only after
# - make a boinc user for the public address that attempts compute, and assign credit
# - correctly implement elapsed time fields
# - distinguish between error types
