# local_cache

share local cache between different processes.

usage example
```
from local_cache import ttl_cache

@ttl_cache(10)
def some_func(*args, **kwargs):
    pass
```
