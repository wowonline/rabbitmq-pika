import os, sys
import subprocess


K = int(sys.argv[1])
N = int(sys.argv[2])


master = subprocess.Popen(
    'python3 master.py'.split(),
    close_fds=True,
)

replicas = []
for i in range(K):
    env_vars = dict(os.environ, **{"number_of_replica" : str(i)})
    replicas.append(
        subprocess.Popen(
            'python3 replica.py'.split(),
            close_fds=True,
            env=env_vars
        )
    )

env_vars = dict(
    os.environ, **{
        "K" : str(K),
        "N" : str(N),
    }
)
node = subprocess.Popen(
    'python3 node.py'.split(),
    close_fds=True,
    env=env_vars
)


for to_wait in [master, node, *replicas]:
    to_wait.communicate()
