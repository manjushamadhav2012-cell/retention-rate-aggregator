import time

# --- Utility Functions to calculate time taken by functions ---
def timed_call(func, *args, **kwargs):
    """
    Utility to measure and print the time taken by a function call.
    Returns the function's result.
    """
    start = time.time()
    result = func(*args, **kwargs)
    elapsed = time.time() - start
    print(f"Time taken for {func.__name__}: {elapsed:.2f} seconds \n")
    return result