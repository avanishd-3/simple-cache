import cProfile
import pstats
import io
import inspect
from functools import wraps

def profile(func=None, output_file=None):
    def decorator(f):
        if inspect.iscoroutinefunction(f):
            @wraps(f)
            async def async_wrapper(*args, **kwargs):
                pr = cProfile.Profile()
                pr.enable()
                result = await f(*args, **kwargs)
                pr.disable()
                s = io.StringIO()
                ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
                ps.print_stats(20)
                print(s.getvalue())
                if output_file:
                    ps.dump_stats(output_file)
                    print(f"Profile data saved to {output_file}")
                return result
            return async_wrapper
        else:
            @wraps(f)
            def sync_wrapper(*args, **kwargs):
                pr = cProfile.Profile()
                pr.enable()
                result = f(*args, **kwargs)
                pr.disable()
                s = io.StringIO()
                ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
                ps.print_stats(20)
                print(s.getvalue())
                if output_file:
                    ps.dump_stats(output_file)
                    print(f"Profile data saved to {output_file}")
                return result
            return sync_wrapper

    if func is None:
        return decorator
    return decorator(func)