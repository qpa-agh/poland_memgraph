import multiprocessing

def execute_with_pool(function, data, max_processes=10):
    with multiprocessing.Pool(min(len(data), max_processes)) as pool:
        pool.starmap(function, [(q,) for q in data])
        pool.close()
        pool.join()
