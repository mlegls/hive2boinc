#!/usr/bin/env python
import sys
import asyncio
import uuid

# config
MYSQL_USER = "boincadm"
MYSQL_PASS = "boincpass"
MYSQL_DB = "test"
MYSQL_APP = "hive_test"

# script
async def sh(cmd) -> tuple[list[str], int]:
    p = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )

    out = []
    async for line in p.stdout:
        decoded = line.decode().strip()
        print(decoded, end='')
        out += [decoded]

    return out, await p.wait()

def mysql_run(cmd):
    return asyncio.run(sh(f"mysql -u {MYSQL_USER} -p{MYSQL_PASS} {MYSQL_DB} -e '{cmd}'"))

if __name__ == '__main__':
    print("running hive job")
    uuid = uuid.uuid4()
    out, code = mysql_run("""
                          insert into workunit (
                            create_time, appid, name, batch, 
                            rsc_fpops_est, rsc_fpops_bound, rsc_memory_bound, rsc_disk_bound, rsc_bandwidth_bound, 
                            need_validate, canonical_resultid, canonical_credit, transition_time, delay_bound, error_mask, file_delete_state, assimilate_state, hr_class, opaque, 
                            min_quorum, target_nresults, max_error_results, max_total_results, max_success_results, 
                            result_template_file, priority, mod_time, fileset_id, app_version_id, transitioner_flags, size_class, keywords, app_version_num
                          )
                          
                          values (
                            UNIX_TIMESTAMP(), (select id from app where name = '{MYSQL_APP}'), 'hive_{uuid}', 0, 
                            0.0, 0.0, 0.0, 0.0, 0.0, 
                            0, 0, 0.0, 0, 0, 0, 0, 0, 0, 0.0, 
                            1, 1, 1, 1, 1, 
                            '', 0, 0, 0, 0, 0, 0, 'hive', 1
                          )""")
    out, code = asyncio.run(sh(f"hive run {" ".join(sys.argv[1:])}"))

    if "✅  Results accepted. Downloading result..." in out:
        # TODO make less fragile; maybe implement json-mode for hive run
        ipfs_addr = out[-1].split("/")[-1]
        res_dir = out[-4].split(" ")[-1]
        res_out, res_err = f"{res_dir}/stdout", f"{res_dir}/stderr"

        print("job succeeded")
        print(f"IPFS address: {ipfs_addr}")
        with open(res_out, 'r') as f:
            print(f.read())

        sys.exit(0)
    else:
        print("job failed")
        sys.exit(1)