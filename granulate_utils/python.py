DETECTED_PYTHON_PROCESSES_REGEX = r"(?:^.+/(?:lib)?python[^/]*$)|(?:^.+/site-packages/.+?$)|(?:^.+/dist-packages/.+?$)"

_BLACKLISTED_PYTHON_PROCS = ["unattended-upgrades", "networkd-dispatcher", "supervisord", "tuned"]
