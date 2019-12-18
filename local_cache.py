# coding=utf-8
import functools
import time
import os
import pickle
import fcntl
import errno
import logging


def ttl_cache(ttl, cache_dir='/tmp/'):
    """
    share local file cache between multi processes
    :param ttl: time to live, seconds
    :param cache_dir: cache directory for storing cache files
    :return:
    """
    def wrap(fn):
        @functools.wraps(fn)
        def fn_wrapped(*args, **kwargs):
            def regenerate_cache(_cache_file_path):
                fn_result = fn(*args, **kwargs)
                write_fd = os.open(cache_file_path, os.O_RDWR | os.O_CREAT)
                fcntl.flock(write_fd, fcntl.LOCK_EX)
                with os.fdopen(fd, 'w') as fn_cache_file:
                    pickle.dump(fn_result, fn_cache_file)
                    fcntl.flock(fd, fcntl.LOCK_UN)
                return fn_result

            func_name = '%s.%s' % (fn.__module__, fn.__name__)

            key = str(":".join([func_name, str(args), str(kwargs)]))
            cache_file_path = os.path.join(cache_dir, key)
            if os.path.exists(cache_file_path):
                # cache file already exists
                now = time.time()
                modify_time = os.path.getmtime(cache_file_path)
                if modify_time + ttl > now:
                    # cache is not expired
                    is_cache_valid = True
                    result = None
                    open_mode = os.O_RDONLY
                    fd = os.open(cache_file_path, open_mode)
                    fcntl.flock(fd, fcntl.LOCK_SH)
                    with os.fdopen(fd, 'r') as cache_file:
                        try:
                            result = pickle.load(cache_file)
                        except EOFError:
                            # something goes wrong with cache data, regenerate cache
                            logging.warn('eof error found for %s', cache_file_path)
                            is_cache_valid = False
                        finally:
                            fcntl.flock(fd, fcntl.LOCK_UN)
                    if is_cache_valid is False:
                        logging.warn('cache invalid, regenerate cache for %s', cache_file_path)
                        result = regenerate_cache(cache_file_path)
                    return result
                else:
                    # cache has expired
                    logging.info('cache expired, regenerate cache for %s', cache_file_path)
                    result = regenerate_cache(cache_file_path)
                    return result
            else:
                # cache file not existed yet
                result = fn(*args, **kwargs)
                open_mode = os.O_RDWR | os.O_CREAT | os.O_EXCL
                try:
                    fd = os.open(cache_file_path, open_mode)
                except OSError as e:
                    if e.errno == errno.EEXIST:
                        logging.warn('cache file %s is already created', cache_file_path)
                    else:
                        raise
                else:
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX)
                    except (IOError, OSError):
                        os.close(fd)
                    else:
                        with os.fdopen(fd, 'w') as cache_file:
                            pickle.dump(result, cache_file)
                            fcntl.flock(fd, fcntl.LOCK_UN)
                return result

        return fn_wrapped

    return wrap
