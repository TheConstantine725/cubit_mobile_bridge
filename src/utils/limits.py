from psutil import cpu_count


def __safe_threading_limit() -> int:
    count_cpus = cpu_count()
    if count_cpus is None:
        return 1
    return int(count_cpus / 2)

