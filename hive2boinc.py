#!/usr/bin/env python
import sys
import asyncio

# config
MYSQL_USER = "boincadm"
MYSQL_PASS = "boincpass"
MYSQL_DB = "test" # project name
APP_NAME = "hive_test"

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

if __name__ == '__main__':
    print("running hive job")
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