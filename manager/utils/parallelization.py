import multiprocessing
import concurrent


def execute_with_pool(function, data, max_processes=10):
    with multiprocessing.Pool(min(len(data), max_processes)) as pool:
        pool.starmap(function, [(q,) for q in data])
        pool.close()
        pool.join()


def parrarelize_processes(function, args_list, n_executors=5):
    assert n_executors < 30
    with concurrent.futures.ProcessPoolExecutor(
        max_workers=min(n_executors, len(args_list))
    ) as executor:
        future_to_id = {
            executor.submit(function, *args): id for id, args in enumerate(args_list)
        }
        for future in concurrent.futures.as_completed(future_to_id):
            id = future_to_id[future]
            yield (id, future.result())
            del future_to_id[future]
